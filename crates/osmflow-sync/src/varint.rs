//! Variable-length integers: LEB128, 7 bits per byte, low first, the high bit set on all but the last byte. Signed
//! numbers are zigzag-encoded first, so small steps either way are short.

/// Appends `v`.
#[expect(clippy::cast_possible_truncation, reason = "the low 7 bits, by design")]
pub(crate) fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Reads one number from the front of `b`, leaving the rest; 0 if `b` is empty.
pub(crate) fn read_varint(b: &mut &[u8]) -> u64 {
    let mut v = 0;
    let mut shift = 0;
    while let Some((&byte, rest)) = b.split_first() {
        *b = rest;
        // Bits past 64, which only damaged bytes have, are dropped
        v |= u64::from(byte & 0x7f).checked_shl(shift).unwrap_or(0);
        if byte < 0x80 {
            break;
        }
        shift += 7;
    }
    v
}

/// Maps a signed number to an unsigned one, 0, -1, 1, -2, ... to 0, 1, 2, 3, ...: `(d << 1) ^ (d >> 63)`.
pub(crate) fn zigzag(d: i64) -> u64 {
    ((d << 1) ^ (d >> 63)).cast_unsigned()
}

/// The number [`zigzag`] made `v` of.
pub(crate) fn unzigzag(v: u64) -> i64 {
    (v >> 1).cast_signed() ^ -((v & 1).cast_signed())
}

/// Appends IDs as the step from the one before (from 0 for the first), zigzagged, as varints.
pub(crate) fn pack_ids(out: &mut Vec<u8>, ids: impl Iterator<Item = i64>) {
    let mut prev = 0_i64;
    for id in ids {
        varint(out, zigzag(id.wrapping_sub(prev)));
        prev = id;
    }
}

/// The IDs [`pack_ids`] wrote.
pub(crate) fn unpack_ids(mut b: &[u8]) -> Vec<i64> {
    let mut prev = 0_i64;
    let mut ids = Vec::new();
    while !b.is_empty() {
        prev = prev.wrapping_add(unzigzag(read_varint(&mut b)));
        ids.push(prev);
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packed(ids: &[i64]) -> Vec<u8> {
        let mut b = Vec::new();
        pack_ids(&mut b, ids.iter().copied());
        b
    }

    #[test]
    fn ids_round_trip() {
        let big = 1_i64 << 36;
        for ids in [
            &[][..],
            &[7],
            &[1, 2, 3, 1000, 1001],
            &[1000, 999, 5, 4],
            &[big, big + 1, big - 3, big + 70_000],
            &[big, -big, 3, i64::MAX, i64::MIN, 0],
        ] {
            assert_eq!(unpack_ids(&packed(ids)), ids);
        }
        assert_eq!(packed(&[]), b"");
    }

    #[test]
    fn ids_have_known_bytes() {
        // Steps 100, +1, -2, 300 zigzag to 200, 2, 3, 600: LEB128 of 200 is c8 01, of 600 d8 04
        assert_eq!(
            packed(&[100, 101, 99, 399]),
            [0xc8, 0x01, 0x02, 0x03, 0xd8, 0x04]
        );
    }
}
