//! Shared, cross-cutting helpers (PEM normalization and related string utilities).

/// Normalizes a PEM string that may have been collapsed into a single line.
pub fn normalize_pem(pem: &str) -> String {
    let pem = pem.replace("\\n", "\n");
    let normalized = pem
        .trim_end()
        .replace("----- ", "-----\n")
        .replace(" -----", "\n-----");
    let trimmed = normalized
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    trimmed + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_pem_expands_literal_backslash_n_collapsed_single_line() {
        let collapsed = "-----BEGIN CERTIFICATE----- abc -----END CERTIFICATE-----";
        let result = normalize_pem(collapsed);
        assert_eq!(
            "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n",
            result
        );
    }

    #[test]
    fn normalize_pem_expands_literal_escape_sequences_in_collapsed_pem() {
        let escaped = "-----BEGIN CERTIFICATE-----\\nabc\\n-----END CERTIFICATE-----";
        let result = normalize_pem(escaped);
        assert_eq!(
            "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n",
            result
        );
    }

    #[test]
    fn normalize_pem_passes_through_well_formed_pem_with_single_trailing_newline() {
        let well_formed = "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n";
        let result = normalize_pem(well_formed);
        assert_eq!(well_formed, result);
    }

    #[test]
    fn normalize_pem_trims_trailing_whitespace_from_lines() {
        let input = "-----BEGIN CERTIFICATE-----\nabc   \n-----END CERTIFICATE-----\n";
        let result = normalize_pem(input);
        assert_eq!(
            "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n",
            result
        );
    }
}
