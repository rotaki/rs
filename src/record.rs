/// A gensort record: 10-byte key + 90-byte payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rec {
    pub key: [u8; 10],
    pub payload: [u8; 90],
}

impl Rec {
    pub fn new(key: [u8; 10], payload: [u8; 90]) -> Self {
        Self { key, payload }
    }

    pub const SIZE: usize = 100; // 10 + 90 bytes
    pub const KEY_SIZE: usize = 10;
    pub const PAYLOAD_SIZE: usize = 90;
}

/// Item in the heap, tagged with generation to implement freezing.
#[derive(Clone, Debug)]
pub struct Item {
    pub rec: Rec,
    pub g: u64,   // current run == current_gen, future runs have gen > current_gen
    pub seq: u64, // tie-breaker for total order
}

impl Item {
    pub fn new(rec: Rec, g: u64, seq: u64) -> Self {
        Self { rec, g, seq }
    }
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.g == other.g && self.seq == other.seq && self.rec.key == other.rec.key
    }
}

impl Eq for Item {}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary by generation, then by key, then by seq.
        // With Reverse in BinaryHeap, this becomes a min-heap by (gen, key, seq),
        // ensuring we fully drain the current generation before considering future ones.
        match self.g.cmp(&other.g) {
            std::cmp::Ordering::Equal => match self.rec.key.cmp(&other.rec.key) {
                std::cmp::Ordering::Equal => self.seq.cmp(&other.seq),
                o => o,
            },
            o => o,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rec_creation() {
        let key = [1u8; 10];
        let payload = [2u8; 90];
        let rec = Rec::new(key, payload);
        assert_eq!(rec.key, key);
        assert_eq!(rec.payload, payload);
    }

    #[test]
    fn test_item_ordering() {
        let rec1 = Rec::new([1u8; 10], [0u8; 90]);
        let rec2 = Rec::new([2u8; 10], [0u8; 90]);

        let item1 = Item::new(rec1.clone(), 0, 0);
        let item2 = Item::new(rec2.clone(), 0, 0);

        assert!(item1 < item2, "Items should be ordered by key");
    }

    #[test]
    fn test_item_generation_ordering() {
        let rec = Rec::new([1u8; 10], [0u8; 90]);

        let item_gen0 = Item::new(rec.clone(), 0, 0);
        let item_gen1 = Item::new(rec.clone(), 1, 0);

        assert!(
            item_gen0 < item_gen1,
            "Same key, lower generation should come first"
        );
    }

    #[test]
    fn test_item_seq_ordering() {
        let rec = Rec::new([1u8; 10], [0u8; 90]);

        let item_seq0 = Item::new(rec.clone(), 0, 0);
        let item_seq1 = Item::new(rec.clone(), 0, 1);

        assert!(
            item_seq0 < item_seq1,
            "Same key and gen, lower seq should come first"
        );
    }
}
