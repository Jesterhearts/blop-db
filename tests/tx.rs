use std::collections::BTreeSet;

use blop_db::BuildError;
use blop_db::Transaction;
use blop_db::tx;

// This decoder follows DESIGN.md appendices A, B and C, not the macro's
// encoder.
#[derive(Debug, PartialEq, Eq)]
enum Ty {
    Unit,
    Bool,
    I64,
    U64,
    Bytes(u32),
    String(u32),
    Tuple(Vec<Ty>),
    Rows(u32, Box<Ty>, Box<Ty>),
}

#[derive(Debug, PartialEq, Eq)]
struct Instruction<'a> {
    opcode: u8,
    operands: &'a [u8],
}

#[derive(Debug)]
struct Program<'a> {
    result: Ty,
    tables: Vec<u64>,
    arguments: Vec<Ty>,
    registers: Vec<Ty>,
    constants: Vec<(Ty, &'a [u8])>,
    instructions: Vec<Instruction<'a>>,
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
) -> &'a [u8] {
    let (prefix, rest) = input.split_at(length);
    *input = rest;
    prefix
}

fn word(
    bytes: &[u8],
    offset: usize,
) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn dword(
    bytes: &[u8],
    offset: usize,
) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn blob<'a>(input: &mut &'a [u8]) -> &'a [u8] {
    let length = dword(take(input, 4), 0) as usize;
    take(input, length)
}

fn type_node(input: &mut &[u8]) -> Ty {
    match take(input, 1)[0] {
        0 => Ty::Unit,
        1 => Ty::Bool,
        2 => Ty::I64,
        3 => Ty::U64,
        4 => Ty::Bytes(dword(take(input, 4), 0)),
        5 => Ty::String(dword(take(input, 4), 0)),
        6 => {
            let count = word(take(input, 2), 0);
            assert!(count <= 256);
            Ty::Tuple((0..count).map(|_| type_node(input)).collect())
        }
        0x20 => {
            let count = dword(take(input, 4), 0);
            assert!(count <= 65_535);
            Ty::Rows(
                count,
                Box::new(type_node(input)),
                Box::new(type_node(input)),
            )
        }
        tag => panic!("unknown type tag {tag:#04x}"),
    }
}

fn descriptor(input: &mut &[u8]) -> Ty {
    let mut bytes = blob(input);
    assert!(bytes.len() <= 65_536);
    assert_eq!(take(&mut bytes, 2), [1, 0]);
    let ty = type_node(&mut bytes);
    assert!(bytes.is_empty(), "trailing descriptor bytes");
    ty
}

fn check_value(
    ty: &Ty,
    input: &mut &[u8],
) {
    match ty {
        Ty::Unit => {}
        Ty::Bool => assert!(take(input, 1)[0] <= 1),
        Ty::I64 | Ty::U64 => {
            take(input, 8);
        }
        Ty::Bytes(bound) | Ty::String(bound) => {
            let bytes = blob(input);
            assert!(bytes.len() <= *bound as usize);
            if matches!(ty, Ty::String(_)) {
                std::str::from_utf8(bytes).unwrap();
            }
        }
        Ty::Tuple(fields) => {
            for field in fields {
                check_value(field, input);
            }
        }
        Ty::Rows(..) => panic!("Rows cannot be an argument or constant"),
    }
}

fn decode(transaction: &Transaction) -> Program<'_> {
    let mut input = transaction.program_bytes();
    assert!(input.len() <= 16 * 1024 * 1024);
    let header = take(&mut input, 32);
    assert_eq!(&header[..8], b"BLOPVM01");
    assert_eq!(word(header, 8), 1);
    assert_eq!(word(header, 10), 0);
    assert_eq!(
        dword(header, 12) as usize,
        transaction.program_bytes().len()
    );
    assert_eq!(dword(header, 28), 0);
    let result = descriptor(&mut input);
    let tables: Vec<_> = (0..word(header, 20))
        .map(|_| u64::from_le_bytes(take(&mut input, 8).try_into().unwrap()))
        .collect();
    assert!(tables.iter().all(|id| *id != 0 && *id != u64::MAX));
    assert!(tables.windows(2).all(|pair| pair[0] < pair[1]));
    let arguments: Vec<_> = (0..word(header, 18))
        .map(|_| descriptor(&mut input))
        .collect();
    let registers = (0..word(header, 16))
        .map(|_| descriptor(&mut input))
        .collect();
    let constants = (0..word(header, 22))
        .map(|_| {
            let ty = descriptor(&mut input);
            let bytes = blob(&mut input);
            let mut remaining = bytes;
            check_value(&ty, &mut remaining);
            assert!(remaining.is_empty(), "trailing constant bytes");
            (ty, bytes)
        })
        .collect();
    let count = dword(header, 24);
    assert!((1..=65_535).contains(&count));
    let instructions = (0..count)
        .map(|_| {
            let header = take(&mut input, 4);
            assert_eq!(header[1], 0, "instruction flags");
            let operands = take(&mut input, usize::from(word(header, 2)));
            let length = match header[0] {
                0x01..=0x03
                | 0x15..=0x17
                | 0x2b
                | 0x33
                | 0x44
                | 0x49
                | 0x50
                | 0x53..=0x55
                | 0x60
                | 0x63 => 4,
                0x10..=0x14
                | 0x20..=0x24
                | 0x28..=0x2a
                | 0x30..=0x32
                | 0x34..=0x35
                | 0x40..=0x43
                | 0x4a..=0x4b
                | 0x51
                | 0x59
                | 0x61..=0x62 => 6,
                0x48 => 22,
                0x52 => 8,
                0x58 => 4 + 2 * usize::from(word(operands, 2)),
                0x64 => 2,
                opcode => panic!("unknown ISA 1 opcode {opcode:#04x}"),
            };
            assert_eq!(operands.len(), length, "opcode {:#04x}", header[0]);
            Instruction {
                opcode: header[0],
                operands,
            }
        })
        .collect();
    assert!(
        input.is_empty(),
        "trailing program bytes or instruction padding"
    );

    let mut input = transaction.argument_bytes();
    assert_eq!(dword(take(&mut input, 4), 0) as usize, arguments.len());
    for ty in &arguments {
        let mut value = blob(&mut input);
        check_value(ty, &mut value);
        assert!(value.is_empty(), "trailing argument bytes");
    }
    assert!(input.is_empty(), "trailing argument section bytes");

    let program = Program {
        result,
        tables,
        arguments,
        registers,
        constants,
        instructions,
    };
    check_control_flow(&program);
    program
}

fn check_control_flow(program: &Program<'_>) {
    let count = program.instructions.len();
    let mut incoming = vec![None; count];
    incoming[0] = Some(vec![false; program.registers.len()]);
    for (index, instruction) in program.instructions.iter().enumerate() {
        let mut initialized = incoming[index].take().expect("unreachable instruction");
        let operands = instruction.operands;
        let words: Vec<_> = operands
            .as_chunks::<2>()
            .0
            .iter()
            .map(|bytes| usize::from(word(bytes, 0)))
            .collect();
        let opcode = instruction.opcode;
        let (destination, sources) = match opcode {
            0x01 | 0x02 => {
                let pool_len = if opcode == 1 {
                    program.constants.len()
                } else {
                    program.arguments.len()
                };
                assert!(words[1] < pool_len);
                (Some(words[0]), vec![])
            }
            0x03 | 0x15..=0x17 | 0x2b | 0x33 | 0x49 | 0x50 | 0x53..=0x55 => {
                (Some(words[0]), vec![words[1]])
            }
            0x40 | 0x41 => {
                assert!(words[1] < program.tables.len());
                (Some(words[0]), vec![words[2]])
            }
            0x42..=0x44 => {
                assert!(words[0] < program.tables.len());
                (None, words[1..].to_vec())
            }
            0x48 => {
                assert!(words[1] < program.tables.len());
                assert_eq!(operands[8] & !3, 0);
                assert_eq!(operands[9], 0);
                for endpoint in 0..2 {
                    if words[2 + endpoint] == 0xffff {
                        assert_eq!(operands[8] & (1 << endpoint), 0);
                    }
                }
                (
                    Some(words[0]),
                    words[2..4]
                        .iter()
                        .copied()
                        .filter(|r| *r != 0xffff)
                        .collect(),
                )
            }
            0x58 => (Some(words[0]), words[2..].to_vec()),
            0x59 => {
                let Ty::Tuple(fields) = &program.registers[words[1]] else {
                    panic!("FIELD source is not a tuple")
                };
                assert!(words[2] < fields.len());
                (Some(words[0]), vec![words[1]])
            }
            0x60 | 0x63 => (None, vec![]),
            0x61 | 0x62 | 0x64 => (None, vec![words[0]]),
            _ => (Some(words[0]), words[1..].to_vec()),
        };
        for source in sources {
            assert!(
                initialized[source],
                "instruction {index} reads uninitialized r{source}"
            );
        }
        if let Some(destination) = destination {
            initialized[destination] = true;
        }
        let successors = match opcode {
            0x60 | 0x61 => {
                let offset = if opcode == 0x60 { 0 } else { 2 };
                let target = dword(operands, offset) as usize;
                assert!(
                    target > index && target < count,
                    "invalid forward target at {index}"
                );
                if opcode == 0x60 {
                    vec![target]
                } else {
                    vec![index + 1, target]
                }
            }
            0x63 | 0x64 => vec![],
            _ => vec![index + 1],
        };
        for successor in successors {
            assert!(successor < count, "path falls off the instruction array");
            if let Some(prior) = &mut incoming[successor] {
                for (prior, current) in prior.iter_mut().zip(&initialized) {
                    *prior &= current;
                }
            } else {
                incoming[successor] = Some(initialized.clone());
            }
        }
    }
}

#[test]
fn return_42_matches_the_complete_golden_program() {
    let transaction = tx! { -> i64 { return 42; } }.unwrap();
    let expected = [
        0x42, 0x4c, 0x4f, 0x50, 0x56, 0x4d, 0x30, 0x31, 1, 0, 0, 0, 79, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 2, 3, 0, 0, 0, 1, 0, 2, 3, 0, 0, 0, 1, 0,
        2, 8, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0x64, 0, 2, 0, 0, 0,
    ];
    assert_eq!(transaction.program_bytes(), expected);
    assert_eq!(transaction.argument_bytes(), [0; 4]);
    let program = decode(&transaction);
    assert_eq!(program.result, Ty::I64);
    assert_eq!(program.registers, [Ty::I64]);
    assert_eq!(
        program.constants,
        [(Ty::I64, 42_i64.to_le_bytes().as_slice())]
    );
    assert_eq!(tx! { return 42; }.unwrap(), transaction);
}

#[test]
fn literal_pools_preserve_integer_limits_utf8_and_nested_tuple_types() {
    let transaction = tx! {
        let text: string<8> = "\u{e9}\0";
        let raw: bytes<8> = b"\0\xff";
        let pair: (i64, u64) = (-9223372036854775808_i64, 18446744073709551615_u64);
        return (text, raw, pair.0, field(pair, 1), true, ());
    }
    .unwrap();
    let program = decode(&transaction);
    let expected: &[(Ty, &[u8])] = &[
        (Ty::String(3), &[3, 0, 0, 0, 0xc3, 0xa9, 0]),
        (Ty::Bytes(2), &[2, 0, 0, 0, 0, 0xff]),
        (Ty::I64, &[0, 0, 0, 0, 0, 0, 0, 0x80]),
        (Ty::U64, &[0xff; 8]),
        (Ty::Bool, &[1]),
        (Ty::Unit, &[]),
    ];
    assert_eq!(program.constants, expected);
    assert_eq!(
        program.result,
        Ty::Tuple(vec![
            Ty::String(8),
            Ty::Bytes(8),
            Ty::I64,
            Ty::U64,
            Ty::Bool,
            Ty::Unit,
        ])
    );
    let tuple = program
        .instructions
        .iter()
        .find(|i| i.opcode == 0x58)
        .unwrap();
    assert_eq!(tuple.operands, [6, 0, 2, 0, 4, 0, 5, 0]);
    let fields: Vec<_> = program
        .instructions
        .iter()
        .filter(|i| i.opcode == 0x59)
        .map(|i| i.operands)
        .collect();
    assert_eq!(fields, [&[8, 0, 7, 0, 0, 0], &[9, 0, 7, 0, 1, 0]]);
}

#[test]
fn captures_are_canonical_snapshots_in_declaration_order() {
    let mut flag = true;
    let mut signed = -2_i64;
    let unsigned = 0x0102_0304_0506_0708_u64;
    let mut text = String::from("\u{e9}\0");
    let mut bytes = vec![0_u8, 0xff, 0x80];
    let mut nested = (false, (-3_i64, String::from("x\0")), vec![1_u8, 2]);
    let transaction = tx! {
        captures {
            flag: bool = flag, signed: i64 = signed, unsigned: u64 = unsigned,
            text: string<8> = text, bytes: bytes<4> = bytes,
            nested: (bool, (i64, string<5>), bytes<4>) = nested,
        }
        return (flag, $signed, unsigned, text, bytes, nested);
    }
    .unwrap();
    flag = false;
    signed = 99;
    text.clear();
    bytes.clear();
    nested.1.1.clear();
    assert!(!flag && signed == 99 && text.is_empty() && bytes.is_empty() && nested.1.1.is_empty());
    assert_eq!(
        transaction.argument_bytes(),
        [
            6, 0, 0, 0, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            8, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1, 7, 0, 0, 0, 3, 0, 0, 0, 0xc3, 0xa9, 0, 7, 0, 0, 0,
            3, 0, 0, 0, 0, 0xff, 0x80, 21, 0, 0, 0, 0, 0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 2, 0, 0, 0, b'x', 0, 2, 0, 0, 0, 1, 2,
        ]
    );
    let program = decode(&transaction);
    assert_eq!(
        program.arguments,
        [
            Ty::Bool,
            Ty::I64,
            Ty::U64,
            Ty::String(8),
            Ty::Bytes(4),
            Ty::Tuple(vec![
                Ty::Bool,
                Ty::Tuple(vec![Ty::I64, Ty::String(5)]),
                Ty::Bytes(4)
            ]),
        ]
    );
    for (index, instruction) in program.instructions[..6].iter().enumerate() {
        assert_eq!(instruction.opcode, 0x02);
        assert_eq!(word(instruction.operands, 0) as usize, index);
        assert_eq!(word(instruction.operands, 2) as usize, index);
    }
    assert_eq!(program.instructions[6].opcode, 0x58);
    assert_eq!(
        program.instructions[6].operands,
        [6, 0, 6, 0, 0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0]
    );
    assert_eq!(program.result, Ty::Tuple(program.arguments));
}

#[test]
fn binding_new_capture_values_does_not_change_the_program() {
    let build = |value: i64, text: &str| {
        tx! {
            captures { value: i64 = value, text: string<16> = text }
            return tuple($value, text);
        }
        .unwrap()
    };
    let first = build(-1, "one");
    let second = build(i64::MAX, "different");
    decode(&first);
    decode(&second);
    assert_eq!(first.program_bytes(), second.program_bytes());
    assert_ne!(first.argument_bytes(), second.argument_bytes());
}

#[test]
fn capture_expressions_run_once_in_source_order_without_moving_places() {
    let mut events = Vec::new();
    let mut values = [String::from("first"), String::from("second")];
    let transaction = tx! {
        captures {
            first: string<8> = values[{ events.push(1); 0 }],
            second: string<8> = values[{ events.push(2); 1 }],
            temporary: string<8> = { events.push(3); String::from("temp") },
        }
        tables { data: u64 => i64 = { events.push(4); 9_u64 } }
        return tuple(first, first, second, temporary);
    }
    .unwrap();
    assert_eq!(events, [1, 2, 3, 4]);
    values[0].push('!');
    assert_eq!(values, ["first!", "second"]);
    assert_eq!(decode(&transaction).tables, [9]);
    assert_eq!(
        transaction.argument_bytes(),
        [
            3, 0, 0, 0, 9, 0, 0, 0, 5, 0, 0, 0, b'f', b'i', b'r', b's', b't', 10, 0, 0, 0, 6, 0, 0,
            0, b's', b'e', b'c', b'o', b'n', b'd', 8, 0, 0, 0, 4, 0, 0, 0, b't', b'e', b'm', b'p',
        ]
    );
}

#[test]
fn binding_enforces_byte_bounds_and_the_complete_argument_limit() {
    assert_eq!(
        tx! { captures { text: string<1> = "\u{e9}" } },
        Err(BuildError::BoundExceeded {
            max_bytes: 1,
            actual_bytes: 2
        })
    );
    assert_eq!(
        tx! { captures { bytes: bytes<2> = b"abc" } },
        Err(BuildError::BoundExceeded {
            max_bytes: 2,
            actual_bytes: 3
        })
    );
    let nested = (1_u64, (String::from("abc"),));
    assert_eq!(
        tx! { captures { value: (u64, (string<2>,)) = nested } },
        Err(BuildError::BoundExceeded {
            max_bytes: 2,
            actual_bytes: 3
        })
    );
    let empty = tx! { captures { text: string<0> = "", bytes: bytes<0> = b"" } }.unwrap();
    decode(&empty);

    let bytes = vec![0_u8; 16 * 1024 * 1024 - 11];
    let build = |value: &[u8]| tx! { captures { value: bytes<16777212> = value } };
    let at_limit = build(&bytes[..bytes.len() - 1]).unwrap();
    assert_eq!(at_limit.argument_bytes().len(), 16 * 1024 * 1024);
    decode(&at_limit);
    assert_eq!(build(&bytes), Err(BuildError::ArgumentsTooLarge));
}

#[test]
fn binding_rejects_reserved_and_duplicate_table_ids() {
    for id in [0, u64::MAX] {
        assert_eq!(
            tx! { tables { data: u64 => i64 = id } },
            Err(BuildError::InvalidTableId(id))
        );
    }
    assert_eq!(
        tx! {
            tables { first: u64 => i64 = 9, second: u64 => i64 = 1, third: u64 => i64 = 9 }
        },
        Err(BuildError::DuplicateTableId(9))
    );
    let transaction = tx! {
        tables { last: u64 => i64 = u64::MAX - 1, first: u64 => i64 = 1 }
    }
    .unwrap();
    assert_eq!(decode(&transaction).tables, [1, u64::MAX - 1]);
}

#[test]
fn table_sorting_remaps_every_address_layout_and_scan_immediates() {
    let build = |ids: [u64; 3]| {
        tx! {
            captures { key: u64 = 5_u64, value: i64 = -3_i64 }
            tables {
                first: u64 => i64 = ids[0], second: u64 => i64 = ids[1], third: u64 => i64 = ids[2],
            }
            first[key];
            exists(second[key]);
            insert(third[key], value);
            store(first[key], value);
            second[key] = value;
            delete(third[key]);
            scan_bounded(first, key, key, 3, 0x1234, 0x01234567);
            scan_bounded(second, unbounded, key, 2, 2, 128);
            scan_bounded(third, key, unbounded, 1, 3, 256);
            scan_bounded(third, unbounded, unbounded, 0, 0, 0);
        }
        .unwrap()
    };
    let original = build([90, 10, 50]);
    let rebound = build([10, 50, 90]);
    let program = decode(&original);
    let other = decode(&rebound);
    assert_eq!(program.tables, [10, 50, 90]);
    assert_eq!(program.tables, other.tables);
    assert_eq!(program.result, other.result);
    assert_eq!(program.arguments, other.arguments);
    assert_eq!(program.registers, other.registers);
    assert_eq!(program.constants, other.constants);
    assert_eq!(original.argument_bytes(), rebound.argument_bytes());
    let expected: &[(u8, &[u8])] = &[
        (0x40, &[2, 0, 2, 0, 0, 0]),
        (0x41, &[3, 0, 0, 0, 0, 0]),
        (0x42, &[1, 0, 0, 0, 1, 0]),
        (0x43, &[2, 0, 0, 0, 1, 0]),
        (0x43, &[0, 0, 0, 0, 1, 0]),
        (0x44, &[1, 0, 0, 0]),
        (
            0x48,
            &[
                4, 0, 2, 0, 0, 0, 0, 0, 3, 0, 0x34, 0x12, 0, 0, 0x67, 0x45, 0x23, 1, 0, 0, 0, 0,
            ],
        ),
        (
            0x48,
            &[
                5, 0, 0, 0, 0xff, 0xff, 0, 0, 2, 0, 2, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0,
            ],
        ),
        (
            0x48,
            &[
                6, 0, 1, 0, 0, 0, 0xff, 0xff, 1, 0, 3, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            ],
        ),
        (
            0x48,
            &[
                7, 0, 1, 0, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
        ),
    ];
    assert_eq!(program.instructions.len(), other.instructions.len());
    for (instruction, &(opcode, operands)) in program.instructions[2..12].iter().zip(expected) {
        assert_eq!(instruction.opcode, opcode);
        assert_eq!(instruction.operands, operands);
    }
    for (before, after) in program.instructions.iter().zip(&other.instructions) {
        assert_eq!(before.opcode, after.opcode);
        let mut expected = before.operands.to_vec();
        let offset = match before.opcode {
            0x40 | 0x41 | 0x48 => Some(2),
            0x42..=0x44 => Some(0),
            _ => None,
        };
        if let Some(offset) = offset {
            let remapped = [1_u16, 2, 0][usize::from(word(&expected, offset))];
            expected[offset..offset + 2].copy_from_slice(&remapped.to_le_bytes());
        }
        assert_eq!(after.operands, expected);
    }
    assert_eq!(
        program.registers[4],
        Ty::Rows(0x1234, Box::new(Ty::U64), Box::new(Ty::I64))
    );
}

#[test]
fn syntax_emits_every_isa_1_opcode() {
    let transaction = tx! {
        captures { x: i64 = 9_i64, y: i64 = 2_i64, n: u64 = 1_u64, a: bool = true, b: bool = false }
        tables { data: u64 => i64 = 7 }
        let copied = copy(x);
        x + y; x - y; x * y; x / y; x % y; -x;
        to_i64_checked(n); to_u64_checked(x);
        x == y; x < y; x <= y; x > y; x >= y;
        bool_and(a, b); bool_or(a, b); bool_xor(a, b); !a;
        x & y; x | y; x ^ y; !x; n << n; n >> n;
        data[n]; exists(data[n]); insert(data[n], x); store(data[n], y); delete(data[n]);
        let rows = scan_bounded(data, unbounded, unbounded, 0, 2, 128);
        rows_len(rows); rows_key(rows, 0); rows_value(rows, 0);
        byte_len("abc"); concat(b"a", b"b"); slice_bytes(b"abc", 1, 2);
        utf8_bytes("abc"); parse_utf8(b"abc"); sha256(b"abc");
        let pair = tuple(x, n);
        field(pair, 0); pair.1;
        require(a, 0x12345678);
        if a { copy(copied); } else { abort(0x87654321); }
        return x;
    }
    .unwrap();
    let program = decode(&transaction);
    let opcodes: BTreeSet<_> = program.instructions.iter().map(|i| i.opcode).collect();
    let expected = [
        0x01, 0x02, 0x03, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x20, 0x21, 0x22, 0x23,
        0x24, 0x28, 0x29, 0x2a, 0x2b, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x40, 0x41, 0x42, 0x43,
        0x44, 0x48, 0x49, 0x4a, 0x4b, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x58, 0x59, 0x60, 0x61,
        0x62, 0x63, 0x64,
    ];
    assert_eq!(opcodes, BTreeSet::from(expected));
    for (index, instruction) in program.instructions[7..12].iter().enumerate() {
        assert_eq!(instruction.opcode, 0x10 + index as u8);
        assert_eq!(instruction.operands, [7 + index as u8, 0, 0, 0, 1, 0]);
    }
    let require = program
        .instructions
        .iter()
        .find(|i| i.opcode == 0x62)
        .unwrap();
    assert_eq!(require.operands, [3, 0, 0x78, 0x56, 0x34, 0x12]);
    let abort = program
        .instructions
        .iter()
        .find(|i| i.opcode == 0x63)
        .unwrap();
    assert_eq!(abort.operands, [0x21, 0x43, 0x65, 0x87]);
}

#[test]
fn short_circuit_branches_skip_risky_loads_on_the_correct_edge() {
    let and = tx! {
        captures { gate: bool = false, key: u64 = 1_u64 }
        tables { data: u64 => i64 = 7 }
        return gate && data[key] > 0;
    }
    .unwrap();
    let or = tx! {
        captures { gate: bool = true, key: u64 = 1_u64 }
        tables { data: u64 => i64 = 7 }
        return gate || data[key] > 0;
    }
    .unwrap();
    let and = decode(&and);
    let or = decode(&or);
    assert_eq!(
        and.instructions
            .iter()
            .map(|i| i.opcode)
            .collect::<Vec<_>>(),
        [0x02, 0x02, 0x03, 0x61, 0x40, 0x01, 0x23, 0x03, 0x64]
    );
    assert_eq!(and.instructions[3].operands, [0, 0, 8, 0, 0, 0]);
    assert_eq!(
        or.instructions.iter().map(|i| i.opcode).collect::<Vec<_>>(),
        [0x02, 0x02, 0x03, 0x61, 0x60, 0x40, 0x01, 0x23, 0x03, 0x64]
    );
    assert_eq!(or.instructions[3].operands, [0, 0, 5, 0, 0, 0]);
    assert_eq!(or.instructions[4].operands, [9, 0, 0, 0]);
    for program in [&and, &or] {
        assert_eq!(program.result, Ty::Bool);
        assert_eq!(program.instructions[2].operands, [2, 0, 0, 0]);
        assert_eq!(program.instructions.last().unwrap().operands, [2, 0]);
    }
}

#[test]
fn nested_branches_preserve_mutable_locals_and_lexical_capture_scope() {
    let transaction = tx! {
        captures { amount: i64 = 8_i64, condition: bool = true }
        -> i64 {
            let mut amount: i64 = 1;
            { let amount: string<8> = "shadow"; byte_len(amount); }
            if condition {
                if $amount < 0 { return $amount; }
                else if $amount == 0 { abort(7); }
                else { amount += $amount; }
            } else if $amount > 10 {
                amount = 4;
            } else {
                abort;
            }
            return amount;
        }
    }
    .unwrap();
    let program = decode(&transaction);
    assert_eq!(program.registers[3], Ty::I64);
    assert_eq!(program.registers[5], Ty::String(8));
    let returns: Vec<_> = program
        .instructions
        .iter()
        .filter(|i| i.opcode == 0x64)
        .map(|i| word(i.operands, 0))
        .collect();
    assert_eq!(returns, [0, 3]);
    let writes = program
        .instructions
        .iter()
        .filter(|i| i.opcode == 0x03 && word(i.operands, 0) == 3)
        .count();
    assert_eq!(writes, 3);
}

#[test]
fn signed_shifts_do_not_inherit_the_unsigned_count_type() {
    let transaction = tx! {
        captures { value: i64 = -8_i64, count: u64 = 1_u64 }
        value << count; value >> count;
        shl_wrap(value, count); shr(value, count);
        8 << 1_u64; shr(-8_i64, 1_u64);
        let mut local = value;
        local <<= count; local >>= count;
        return local;
    }
    .unwrap();
    let program = decode(&transaction);
    let shifts: Vec<_> = program
        .instructions
        .iter()
        .filter(|i| matches!(i.opcode, 0x34 | 0x35))
        .collect();
    assert_eq!(shifts.len(), 8);
    for shift in shifts {
        for offset in [0, 2] {
            assert_eq!(
                program.registers[usize::from(word(shift.operands, offset))],
                Ty::I64
            );
        }
        assert_eq!(
            program.registers[usize::from(word(shift.operands, 4))],
            Ty::U64
        );
    }
}

#[test]
fn compound_table_assignment_evaluates_the_address_once_before_the_rhs() {
    let transaction = tx! {
        tables { pointers: u64 => u64 = 9, data: u64 => i64 = 3 }
        data[pointers[1]] += data[2];
    }
    .unwrap();
    let program = decode(&transaction);
    assert_eq!(
        program
            .instructions
            .iter()
            .map(|i| i.opcode)
            .collect::<Vec<_>>(),
        [0x01, 0x40, 0x40, 0x01, 0x40, 0x10, 0x43, 0x01, 0x64]
    );
    assert_eq!(program.instructions[1].operands, [1, 0, 1, 0, 0, 0]);
    assert_eq!(program.instructions[2].operands, [2, 0, 0, 0, 1, 0]);
    assert_eq!(program.instructions[4].operands, [4, 0, 0, 0, 3, 0]);
    assert_eq!(program.instructions[6].operands, [0, 0, 1, 0, 5, 0]);
}

#[test]
fn macro_bindings_are_hygienic_and_empty_tuples_remain_distinct_from_unit() {
    use blop_db as renamed;
    let __blop_captures = 7_u64;
    let __blop_tables = String::from("kept");
    let __blop_value = true;
    let __blop_arguments = -1_i64;
    let transaction = renamed::tx! {
        captures {
            a: string<4> = __blop_tables, b: bool = __blop_value,
            c: i64 = __blop_arguments, unit: () = (), empty: tuple<> = (),
        }
        tables { t: u64 => i64 = __blop_captures }
        return (a, b, c, unit, empty, tuple());
    }
    .unwrap();
    let program = decode(&transaction);
    assert_eq!(program.tables, [7]);
    assert_eq!(program.arguments[3..], [Ty::Unit, Ty::Tuple(vec![])]);
    assert_eq!(
        program.result,
        Ty::Tuple(vec![
            Ty::String(4),
            Ty::Bool,
            Ty::I64,
            Ty::Unit,
            Ty::Tuple(vec![]),
            Ty::Tuple(vec![]),
        ])
    );
    assert_eq!(__blop_tables, "kept");
}

#[test]
#[allow(non_camel_case_types)]
fn primitive_aliases_in_the_caller_cannot_change_capture_encodings() {
    type u64 = u32;
    type i64 = i32;
    type bool = u8;
    type u8 = u16;
    type str = String;
    let _: Option<(u64, i64, bool, u8, str)> = None;
    let transaction = tx! {
        captures {
            unsigned: u64 = 7_u64, signed: i64 = -1_i64, flag: bool = true,
            raw: bytes<1> = b"x", text: string<1> = "x",
        }
        tables { t: u64 => i64 = 1_u64 }
        return (unsigned, signed, flag, raw, text);
    }
    .unwrap();
    let program = decode(&transaction);
    assert_eq!(
        program.arguments,
        [Ty::U64, Ty::I64, Ty::Bool, Ty::Bytes(1), Ty::String(1)]
    );
    assert_eq!(program.tables, [1]);
}

#[test]
fn unit_fallthrough_and_terminating_branches_have_no_dead_instructions() {
    let empty = tx! {}.unwrap();
    let explicit = tx! { return; }.unwrap();
    assert_eq!(empty, explicit);
    assert_eq!(decode(&empty).result, Ty::Unit);
    for transaction in [
        tx! { abort; }.unwrap(),
        tx! { abort(); }.unwrap(),
        tx! { if true {} else if false {} else {} }.unwrap(),
        tx! { if true { return 1; } else if false { abort(); } else { return 2; } }.unwrap(),
        tx! { require(true); return; }.unwrap(),
    ] {
        let program = decode(&transaction);
        for instruction in &program.instructions {
            if instruction.opcode == 0x63 {
                assert_eq!(instruction.operands, [0; 4]);
            }
            if instruction.opcode == 0x62 {
                assert_eq!(&instruction.operands[2..], [0; 4]);
            }
        }
    }
    let unit = tx! { return (); }.unwrap();
    let tuple = tx! { return tuple(); }.unwrap();
    assert_eq!(decode(&unit).result, Ty::Unit);
    assert_eq!(decode(&tuple).result, Ty::Tuple(vec![]));
}
