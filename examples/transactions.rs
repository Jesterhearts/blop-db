//! Create accounts, transfer 25 units, and verify balances after reopening.
//!
//! Run `cargo run --example transactions -- /path/to/new-database` from the
//! repository root. The database directory must be new and its parent must
//! exist. The example verifies that both final balances are 75.

#[cfg(any(unix, windows))]
use blop_db::Limits;
#[cfg(any(unix, windows))]
use blop_db::database;
#[cfg(any(unix, windows))]
use blop_db::database::CreateOptions;
#[cfg(any(unix, windows))]
use blop_db::tx;
#[cfg(any(unix, windows))]
use blop_db::vm::CatalogueOperation;
#[cfg(any(unix, windows))]
use blop_db::vm::Outcome;
#[cfg(any(unix, windows))]
use blop_db::vm::Type;
#[cfg(any(unix, windows))]
use blop_db::vm::Value;

#[cfg(any(unix, windows))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: cargo run --example transactions -- /path/to/new-database")?;
    let claims = Limits {
        instructions: 1024,
        writes: 1024,
        overlay_bytes: 8 * 1024 * 1024,
        ..Limits::default()
    };
    let db = database::create(
        &path,
        CreateOptions {
            limits: claims,
            ..CreateOptions::default()
        },
    )
    .await?;
    let created = database::execute_catalogue(
        &db,
        CatalogueOperation::Create {
            name: "balances".into(),
            key: Type::U64,
            value: Type::I64,
        },
    )
    .await?;
    let balances_id = match created.outcome {
        Outcome::Success {
            value: Value::U64(id),
            ..
        } => id,
        outcome => return Err(format!("table creation failed: {outcome:?}").into()),
    };

    let initialized = database::execute(
        &db,
        tx! {
            tables { balances: u64 => i64 = balances_id }
            insert(balances[10], 100);
            insert(balances[20], 50);
        }?,
        claims,
    )
    .await?;
    if let Outcome::Aborted(abort) = initialized.outcome {
        return Err(format!("initialization aborted: {abort:?}").into());
    }

    let from_id = 10_u64;
    let to_id = 20_u64;
    let amount = 25_i64;
    let transfer = tx! {
        captures { from: u64 = from_id, to: u64 = to_id, amount: i64 = amount }
        tables { balances: u64 => i64 = balances_id }
        require(amount > 0, 1);
        require(from != to, 2);
        require(balances[from] >= amount, 3);
        balances[from] -= amount;
        balances[to] += amount;
        return (balances[from], balances[to]);
    }?;
    let receipt = database::execute(&db, transfer, claims).await?;
    match receipt.outcome {
        Outcome::Success { value, .. } => {
            assert_eq!(value, Value::Tuple(vec![Value::I64(75), Value::I64(75)]));
            println!(
                "Durable transfer at sequence {}: {value:?}",
                receipt.sequence
            );
        }
        Outcome::Aborted(abort) => return Err(format!("transfer aborted: {abort:?}").into()),
    }
    database::close(&db).await?;

    let db = database::open(&path).await?;
    // A result-only transaction also enters the log. A snapshot read API is
    // not yet available, so this verifies persisted values through the VM.
    let receipt = database::execute(
        &db,
        tx! {
            tables { balances: u64 => i64 = balances_id }
            return (balances[10], balances[20]);
        }?,
        claims,
    )
    .await?;
    match receipt.outcome {
        Outcome::Success { value, .. } => {
            assert_eq!(value, Value::Tuple(vec![Value::I64(75), Value::I64(75)]));
            println!("Balances after reopening: {value:?}");
        }
        Outcome::Aborted(abort) => return Err(format!("verification aborted: {abort:?}").into()),
    }
    database::close(&db).await?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn main() {
    eprintln!("The database example requires Unix or Windows.");
}
