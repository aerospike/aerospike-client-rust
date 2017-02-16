use std::str::FromStr;

#[derive(Debug)]
pub struct Percent(u8);

#[derive(Debug)]
pub enum Workload {
    // Initialize data with sequential key writes.
    Initialize,

    // Read/Update. Perform random key, random read all bins wor write all bins workload.
    ReadUpdate { read_pct: Percent },
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(s: &str) -> Result<Workload, String> {
        match s {
            "RU" => Ok(Workload::ReadUpdate { read_pct: Percent(50) }),
            "I" => Ok(Workload::Initialize),
            _ => Err(String::from("Invalid workload definition")),
        }
    }
}
