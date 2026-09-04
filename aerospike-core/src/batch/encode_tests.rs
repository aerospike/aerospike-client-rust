// Wire-level tests for the batch encoder's header-repeat compression.
//
// `set_batch_operate` decides once per row whether that row repeats the
// previous row's header, and the two loops that size and then write the
// message share that decision. These tests pin the decision through the bytes
// it produces, so a row that silently stops repeating — or starts repeating
// when it must not — changes the encoded length and fails here.

#![cfg(test)]

use crate::batch::BatchOperation;
use crate::commands::buffer::Buffer;
use crate::operations;
use crate::policy::BatchPolicy;
use crate::{BatchReadPolicy, BatchWritePolicy, Bin, Bins, Key, Value};

/// Deterministic xorshift so a failing seed reproduces exactly.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// The distinct row shapes a batch can mix. Consecutive rows of the same shape
/// repeat; any change of shape — bins, set, namespace, or read vs write — must
/// not.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    ReadA,
    ReadB,
    ReadOtherSet,
    ReadOtherNs,
    Write,
}

fn row_of(shape: Shape, i: i64) -> (BatchOperation, usize) {
    let key = |ns: &str, set: &str| Key::new(ns, set, Value::from(i)).unwrap();
    let read = |k: Key, bins: Bins| BatchOperation::read(&BatchReadPolicy::default(), k, bins);
    let op = match shape {
        Shape::ReadA => read(key("test", "set"), Bins::from(["a", "b"])),
        Shape::ReadB => read(key("test", "set"), Bins::from(["c", "d", "e"])),
        Shape::ReadOtherSet => read(key("test", "set2"), Bins::from(["a", "b"])),
        Shape::ReadOtherNs => read(key("other", "set"), Bins::from(["a", "b"])),
        Shape::Write => BatchOperation::write(
            &BatchWritePolicy::default(),
            key("test", "set"),
            vec![operations::put(&Bin::new("w".to_string(), Value::from(1i64)))],
        ),
    };
    (op, i as usize)
}

/// Random sequences of shapes, with a coin flip per row between "same shape
/// as the previous row" and "a fresh random shape", so repeats and header
/// breaks land at random positions. The oracle is independent of the encoder's
/// bookkeeping: the whole batch must cost exactly the empty-message overhead
/// plus, per row, either one repeat row or that row's own full cost measured by
/// encoding it alone. If `repeats[i]` were ever computed against the wrong
/// predecessor, or the write loop disagreed with the sizing loop, the total
/// would drift from the prediction.
#[test]
fn random_repeat_sequences_encode_to_the_predicted_size() {
    const SHAPES: [Shape; 5] = [
        Shape::ReadA,
        Shape::ReadB,
        Shape::ReadOtherSet,
        Shape::ReadOtherNs,
        Shape::Write,
    ];
    let base = encoded_len(&[]);
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let (mut repeats_seen, mut breaks_seen) = (0usize, 0usize);

    for round in 0..300 {
        let seed = rng.next();
        let mut r = Rng(seed | 1);
        let n = 1 + r.below(30);

        let mut shapes: Vec<Shape> = Vec::with_capacity(n);
        for i in 0..n {
            let same_as_prev = i > 0 && r.below(2) == 0;
            shapes.push(if same_as_prev {
                shapes[i - 1]
            } else {
                SHAPES[r.below(SHAPES.len())]
            });
        }
        let rows: Vec<(BatchOperation, usize)> = shapes
            .iter()
            .enumerate()
            .map(|(i, s)| row_of(*s, i as i64))
            .collect();

        let mut expected = base;
        for i in 0..n {
            let repeats = i > 0 && rows[i].0.match_header(Some(&rows[i - 1].0));
            if repeats {
                repeats_seen += 1;
                expected += REPEAT_ROW_SIZE;
            } else {
                breaks_seen += 1;
                expected += encoded_len(&[rows[i].clone()]) - base;
            }
        }

        let actual = encoded_len(&rows);
        assert_eq!(
            actual, expected,
            "round {round} seed {seed:#x}: shapes {shapes:?} encoded to {actual} bytes, \
             oracle predicts {expected}"
        );
    }

    assert!(repeats_seen > 500 && breaks_seen > 500, "{}", format!("the random mix must exercise both outcomes heavily (repeats {repeats_seen}, breaks {breaks_seen})"));
}

/// Bytes a repeated row costs: batch index (4) + digest (20) + the repeat
/// marker (1). A row that writes its own header instead costs far more.
const REPEAT_ROW_SIZE: usize = 4 + 20 + 1;

fn read_row(i: i64, bins: Bins) -> (BatchOperation, usize) {
    let key = Key::new("test", "set", Value::from(i)).unwrap();
    (
        BatchOperation::read(&BatchReadPolicy::default(), key, bins),
        i as usize,
    )
}

/// Length of the encoded message body.
///
/// `Buffer::end` rewinds `data_offset` and stores the length in the low 48 bits
/// of the 8-byte proto header, so the header is what has to be read back.
fn encoded_len(batch: &[(BatchOperation, usize)]) -> usize {
    use byteorder::{ByteOrder, NetworkEndian};

    let mut buf = Buffer::new(0);
    buf.set_batch_operate(&BatchPolicy::default(), batch)
        .expect("encoding a well-formed batch cannot fail");
    (NetworkEndian::read_u64(&buf.data_buffer[0..8]) & 0xFFFF_FFFF_FFFF) as usize
}

/// Rows sharing a namespace, set, policy and bin list repeat, so each one past
/// the first must add exactly the 25-byte repeat row and nothing else.
#[test]
fn identical_rows_each_cost_only_a_repeat_row() {
    let bins = || Bins::from(["a", "b"]);
    let one = encoded_len(&[read_row(0, bins())]);

    for n in [2usize, 5, 64] {
        let batch: Vec<_> = (0..n as i64).map(|i| read_row(i, bins())).collect();
        assert_eq!(
            encoded_len(&batch) - one,
            REPEAT_ROW_SIZE * (n - 1),
            "{n} identical rows must encode as one full header plus {} repeat rows",
            n - 1
        );
    }
}

/// The negative control: rows that differ in their bin list cannot repeat, so
/// every row carries a full header and costs well more than a repeat row. If
/// the encoder ever repeated these, the bins of the first row would silently be
/// applied to all of them.
#[test]
fn rows_with_different_bins_do_not_repeat() {
    let one = encoded_len(&[read_row(0, Bins::from(["a", "b"]))]);
    let batch = vec![
        read_row(0, Bins::from(["a", "b"])),
        read_row(1, Bins::from(["c", "d", "e"])),
    ];

    let grew_by = encoded_len(&batch) - one;
    assert!(grew_by > REPEAT_ROW_SIZE, "{}", format!("a row with its own bin list must write a full header, but it added only \
         {grew_by} bytes — the cost of a repeat row"));
}

/// A repeat is only valid against the row immediately before it, which is what
/// lets the sizing pass and the writing pass agree. An A, B, A sequence must
/// therefore repeat nothing, even though rows 0 and 2 are identical.
#[test]
fn a_repeat_is_only_against_the_previous_row() {
    let a = || Bins::from(["a", "b"]);
    let b = || Bins::from(["c", "d", "e"]);

    let aa = encoded_len(&[read_row(0, a()), read_row(1, a())]);
    let aba = encoded_len(&[read_row(0, a()), read_row(1, b()), read_row(2, a())]);

    // The third row is identical to the first but does not follow it, so it
    // writes a full header: the A,B,A message must exceed A,A by more than one
    // repeat row.
    assert!(
        aba - aa > REPEAT_ROW_SIZE,
        "row 2 repeated a row it does not directly follow (grew by {})",
        aba - aa
    );
}

/// Rows differing only by namespace share every other field, so the namespace
/// alone must break the repeat — otherwise the second row would be sent against
/// the first row's namespace.
#[test]
fn a_different_namespace_breaks_the_repeat() {
    let bins = || Bins::from(["a", "b"]);
    let other_ns = (
        BatchOperation::read(
            &BatchReadPolicy::default(),
            Key::new("other", "set", Value::from(1i64)).unwrap(),
            bins(),
        ),
        1usize,
    );

    let one = encoded_len(&[read_row(0, bins())]);
    let two = encoded_len(&[read_row(0, bins()), other_ns]);

    assert!(
        two - one > REPEAT_ROW_SIZE,
        "a row in another namespace repeated the previous row's header"
    );
}
