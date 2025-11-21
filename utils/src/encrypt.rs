
pub fn md5(input: &str) -> String {
    format!("{:x}",md5::compute(input.as_bytes()))
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_md5() {
        let input = "5a33fa9e0cd788ff2025445201742fda&1760086002619&12574478&{}";
        let output = md5(input);
        assert_eq!(output, "5574764443ac90fa331a441a58f7a5dd");
    }
}