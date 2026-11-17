/// Smallest `L >= 0` with `ceil(min(rows, columns) / 2^L) <= 64`.
pub fn decomposition_levels(rows: u16, columns: u16) -> u32 {
    let short = rows.min(columns) as u32;
    let mut levels = 0;
    while short.div_ceil(1 << levels) > 64 {
        levels += 1;
    }
    levels
}

/// Index into `Encoded::ends` of the first resolution whose long side is at
/// least `preview_size`, or the last one.
pub fn preview_level(rows: u16, columns: u16, preview_size: u32) -> usize {
    let levels = decomposition_levels(rows, columns);
    let long = rows.max(columns) as u32;
    (0..=levels)
        .find(|r| long.div_ceil(1 << (levels - r)) >= preview_size)
        .unwrap_or(levels) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_rule() {
        assert_eq!(decomposition_levels(512, 512), 3);
        assert_eq!(decomposition_levels(3328, 4096), 6);
        assert_eq!(decomposition_levels(64, 64), 0);
        assert_eq!(decomposition_levels(65, 2000), 1);
        assert_eq!(decomposition_levels(1, 512), 0);
    }

    #[test]
    fn preview_level_for_tiny_frame() {
        assert_eq!(preview_level(32, 32, 256), 0);
    }

    #[test]
    fn preview_level_picks_256() {
        assert_eq!(preview_level(512, 512, 256), 2);
        assert_eq!(preview_level(3328, 4096, 256), 2);
    }
}
