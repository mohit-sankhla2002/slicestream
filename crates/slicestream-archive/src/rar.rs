//! A small safe wrapper over the UnRAR library (`unrar_sys`, the FFI layer
//! of the `unrar` crate).
//!
//! Entries are only ever *tested* (`RAR_TEST`): UnRAR decodes them and hands
//! the data to our callback, it never creates a file, directory or link.
//! The `unrar` crate's own API can only read an entry whole into memory or
//! extract it by path, and extraction follows RAR redirections (file copies
//! and hard links resolved against the process's working directory), so it
//! is not used.
//!
//! UnRAR keeps some process-wide state (its error handler), so every call
//! into it holds one global lock. Callbacks run inside those calls and must
//! not call back into UnRAR.

use std::ffi::c_void;
use std::fmt;
use std::os::raw::{c_char, c_int, c_uint};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

use unrar_sys as sys;

static LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// `UCM_LARGEDICT`: not in `unrar_sys`. Any answer but 1 refuses.
const UCM_LARGEDICT: sys::UINT = 5;
const ERAR_LARGE_DICT: c_int = 25;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Mode {
    /// Headers only.
    List,
    /// Headers and data.
    Extract,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Op {
    /// Move to the next header (solid archives still decode, uncounted).
    Skip,
    /// Decode and verify; the data goes to the callback.
    Test,
}

/// An UnRAR failure.
#[derive(Debug)]
pub(crate) struct Error {
    code: c_int,
    /// UnRAR asked for a password.
    password: bool,
}

impl Error {
    pub(crate) fn encrypted(&self) -> bool {
        self.password
            || self.code == sys::ERAR_MISSING_PASSWORD
            || self.code == sys::ERAR_BAD_PASSWORD
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.code {
            sys::ERAR_NO_MEMORY => "out of memory",
            sys::ERAR_BAD_DATA => "data error (checksum mismatch)",
            sys::ERAR_BAD_ARCHIVE => "not a valid RAR archive",
            sys::ERAR_UNKNOWN_FORMAT => "unknown archive format",
            sys::ERAR_EOPEN => "cannot open archive",
            sys::ERAR_EREAD => "read error",
            sys::ERAR_MISSING_PASSWORD | sys::ERAR_BAD_PASSWORD => "encrypted archive",
            ERAR_LARGE_DICT => "dictionary too large",
            sys::ERAR_UNKNOWN => "unknown error",
            _ => return write!(f, "UnRAR error {}", self.code),
        })
    }
}

/// One file header.
pub(crate) struct Header {
    /// Entry name (for reporting only).
    pub(crate) name: String,
    flags: u32,
    dict_kb: u32,
    redir: u32,
    method: u32,
}

impl Header {
    /// A regular file: not a directory and not a link or file copy.
    pub(crate) fn is_file(&self) -> bool {
        self.flags & sys::RHDF_DIRECTORY == 0 && self.redir == 0
    }
    pub(crate) fn encrypted(&self) -> bool {
        self.flags & sys::RHDF_ENCRYPTED != 0
    }
    /// Dictionary the decoder allocates (none for stored data).
    pub(crate) fn dictionary(&self) -> u64 {
        // DLL reports Method + 0x30; 0x30 is "stored".
        if self.method == 0x30 {
            0
        } else {
            u64::from(self.dict_kb) * 1024
        }
    }
}

/// Callback state; heap-allocated so its address is stable for UnRAR.
struct State {
    /// `*mut &mut dyn FnMut(&[u8]) -> bool` while a `process` runs.
    data: *mut c_void,
    password: bool,
}

// The vendored `dll.hpp` declares its structs under `#pragma pack(1)`;
// `unrar_sys`'s `repr(C)` versions are misaligned from the first pointer on
// (64-bit), so exact copies are declared here and passed by pointer cast.

/// `RARHeaderDataEx`.
#[repr(C, packed)]
struct HeaderDataEx {
    arc_name: [c_char; 1024],
    arc_name_w: [sys::WCHAR; 1024],
    file_name: [c_char; 1024],
    file_name_w: [sys::WCHAR; 1024],
    flags: c_uint,
    pack_size: c_uint,
    pack_size_high: c_uint,
    unp_size: c_uint,
    unp_size_high: c_uint,
    host_os: c_uint,
    file_crc: c_uint,
    file_time: c_uint,
    unp_ver: c_uint,
    method: c_uint,
    file_attr: c_uint,
    cmt_buf: *mut c_char,
    cmt_buf_size: c_uint,
    cmt_size: c_uint,
    cmt_state: c_uint,
    dict_size: c_uint,
    hash_type: c_uint,
    hash: [c_char; 32],
    redir_type: c_uint,
    redir_name: *mut sys::WCHAR,
    redir_name_size: c_uint,
    dir_target: c_uint,
    times: [c_uint; 6],
    arc_name_ex: *mut sys::WCHAR,
    arc_name_ex_size: c_uint,
    file_name_ex: *mut sys::WCHAR,
    file_name_ex_size: c_uint,
    reserved: [c_uint; 982],
}

/// `RAROpenArchiveDataEx`.
#[repr(C, packed)]
struct OpenArchiveDataEx {
    arc_name: *const c_char,
    arc_name_w: *const sys::WCHAR,
    open_mode: c_uint,
    open_result: c_uint,
    cmt_buf: *mut c_char,
    cmt_buf_size: c_uint,
    cmt_size: c_uint,
    cmt_state: c_uint,
    flags: c_uint,
    callback: Option<sys::Callback>,
    user_data: sys::LPARAM,
    op_flags: c_uint,
    cmt_buf_w: *mut sys::WCHAR,
    reserved: [c_uint; 25],
}

// Sizes measured from the vendored header with g++ (x86_64 Linux).
#[cfg(all(target_pointer_width = "64", not(windows)))]
const _: () = {
    assert!(std::mem::size_of::<HeaderDataEx>() == 14340);
    assert!(std::mem::size_of::<OpenArchiveDataEx>() == 176);
    assert!(std::mem::offset_of!(HeaderDataEx, dict_size) == 10304);
    assert!(std::mem::offset_of!(HeaderDataEx, redir_type) == 10344);
    assert!(std::mem::offset_of!(HeaderDataEx, arc_name_ex) == 10388);
    assert!(std::mem::offset_of!(OpenArchiveDataEx, cmt_buf_w) == 68);
};

pub(crate) struct Rar {
    handle: *const sys::Handle,
    /// From `Box::into_raw`; freed after the handle is closed.
    state: *mut State,
    flags: u32,
}

impl Drop for Rar {
    fn drop(&mut self) {
        {
            let _g = lock();
            // SAFETY: `handle` came from RAROpenArchiveEx and is closed once.
            unsafe { sys::RARCloseArchive(self.handle) };
        }
        // SAFETY: allocated by `open`; UnRAR no longer holds it.
        drop(unsafe { Box::from_raw(self.state) });
    }
}

extern "C" fn callback(
    msg: sys::UINT,
    user: sys::LPARAM,
    p1: sys::LPARAM,
    p2: sys::LPARAM,
) -> c_int {
    // Never unwind into C.
    catch_unwind(AssertUnwindSafe(|| {
        if user == 0 {
            return -1;
        }
        // SAFETY: `user` is the `State` box owned by the open `Rar`, which
        // outlives every call that can invoke this callback.
        let st = unsafe { &mut *(user as *mut State) };
        match msg {
            sys::UCM_PROCESSDATA => {
                if st.data.is_null() {
                    return 0;
                }
                if p1 == 0 || p2 <= 0 {
                    return 0;
                }
                // SAFETY: UnRAR passes a buffer of `p2` bytes at `p1`, valid
                // for the duration of the call.
                let data = unsafe { std::slice::from_raw_parts(p1 as *const u8, p2 as usize) };
                // SAFETY: set by `process` to a live `&mut dyn FnMut` for the
                // duration of the UnRAR call that invokes us.
                let f = unsafe { &mut *(st.data as *mut &mut dyn FnMut(&[u8]) -> bool) };
                if f(data) { 0 } else { -1 }
            }
            sys::UCM_NEEDPASSWORD | sys::UCM_NEEDPASSWORDW => {
                st.password = true;
                -1
            }
            // Never open further volumes.
            sys::UCM_CHANGEVOLUME | sys::UCM_CHANGEVOLUMEW => -1,
            UCM_LARGEDICT => 0,
            _ => 0,
        }
    }))
    .unwrap_or(-1)
}

impl Rar {
    pub(crate) fn open(path: &Path, mode: Mode) -> Result<Rar, Error> {
        let fail = Error {
            code: sys::ERAR_EOPEN,
            password: false,
        };
        let Some(p) = path.to_str() else {
            return Err(fail);
        };
        let mut name: Vec<sys::WCHAR> = p.chars().map(|c| c as sys::WCHAR).collect();
        if name.contains(&0) {
            return Err(fail);
        }
        name.push(0);
        let state = Box::into_raw(Box::new(State {
            data: ptr::null_mut(),
            password: false,
        }));
        let mut data = OpenArchiveDataEx {
            arc_name: ptr::null(),
            arc_name_w: name.as_ptr(),
            open_mode: match mode {
                Mode::List => sys::RAR_OM_LIST,
                Mode::Extract => sys::RAR_OM_EXTRACT,
            },
            open_result: 0,
            cmt_buf: ptr::null_mut(),
            cmt_buf_size: 0,
            cmt_size: 0,
            cmt_state: 0,
            flags: 0,
            callback: Some(callback),
            user_data: state as sys::LPARAM,
            op_flags: 0,
            cmt_buf_w: ptr::null_mut(),
            reserved: [0; 25],
        };
        let handle = {
            let _g = lock();
            // SAFETY: `data` matches the C layout and is fully initialised;
            // `name` and `state` outlive the call (and `state` the handle).
            unsafe {
                sys::RAROpenArchiveEx(
                    &mut data as *mut OpenArchiveDataEx as *const sys::OpenArchiveDataEx,
                )
            }
        };
        let code = { data.open_result } as c_int;
        if handle.is_null() {
            // SAFETY: allocated above; UnRAR has no handle that could use it.
            let state = unsafe { Box::from_raw(state) };
            return Err(Error {
                code: if code == 0 { sys::ERAR_EOPEN } else { code },
                password: state.password,
            });
        }
        let rar = Rar {
            handle,
            state,
            flags: { data.flags },
        };
        if code != sys::ERAR_SUCCESS {
            return Err(rar.error(code));
        }
        Ok(rar)
    }

    pub(crate) fn is_volume(&self) -> bool {
        self.flags & sys::ROADF_VOLUME != 0
    }
    pub(crate) fn is_solid(&self) -> bool {
        self.flags & sys::ROADF_SOLID != 0
    }
    pub(crate) fn encrypted_headers(&self) -> bool {
        self.flags & sys::ROADF_ENCHEADERS != 0
    }

    fn error(&self, code: c_int) -> Error {
        // SAFETY: owned by `self`; no UnRAR call is running.
        let password = unsafe { (*self.state).password };
        Error { code, password }
    }

    /// The next file header, or `None` at the end of the archive.
    pub(crate) fn read_header(&mut self) -> Result<Option<Header>, Error> {
        // SAFETY: plain C data; all-zero is its documented initial state.
        let mut buf: Box<HeaderDataEx> = Box::new(unsafe { std::mem::zeroed() });
        let code = {
            let _g = lock();
            // SAFETY: open handle; `buf` matches the C layout and is writable.
            unsafe {
                sys::RARReadHeaderEx(
                    self.handle,
                    &mut *buf as *mut HeaderDataEx as *const sys::HeaderDataEx,
                )
            }
        };
        match code {
            sys::ERAR_SUCCESS => {}
            sys::ERAR_END_ARCHIVE => return Ok(None),
            c => return Err(self.error(c)),
        }
        let name_w = buf.file_name_w;
        let name = name_w
            .iter()
            .take_while(|&&c| c != 0)
            .map(|&c| char::from_u32(c as u32).unwrap_or(char::REPLACEMENT_CHARACTER))
            .collect();
        let h = &*buf;
        Ok(Some(Header {
            name,
            flags: { h.flags },
            dict_kb: { h.dict_size },
            redir: { h.redir_type },
            method: { h.method },
        }))
    }

    /// Process the entry whose header was just read. With `Op::Test` the
    /// decoded data is passed to `on_data`; returning `false` cancels.
    pub(crate) fn process(
        &mut self,
        op: Op,
        on_data: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<(), Error> {
        let mut f: &mut dyn FnMut(&[u8]) -> bool = on_data;
        // SAFETY: owned by `self`; no UnRAR call is running.
        unsafe { (*self.state).data = &mut f as *mut &mut dyn FnMut(&[u8]) -> bool as *mut c_void };
        let code = {
            let _g = lock();
            // SAFETY: open handle; null destination (test/skip write nothing).
            unsafe {
                sys::RARProcessFileW(
                    self.handle,
                    match op {
                        Op::Skip => sys::RAR_SKIP,
                        Op::Test => sys::RAR_TEST,
                    },
                    ptr::null(),
                    ptr::null(),
                )
            }
        };
        // SAFETY: as above; `f` is not used after this.
        unsafe { (*self.state).data = ptr::null_mut() };
        if code == sys::ERAR_SUCCESS {
            Ok(())
        } else {
            Err(self.error(code))
        }
    }
}
