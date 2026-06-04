//! Structs to deserialize received [`TransactionMessage`]s from the
//! `nox_ingestor` NATS JetStream stream.

use alloy_primitives::Address;
use serde::Deserialize;

/// Handle type for encrypted values (hex-encoded bytes32)
pub type Handle = String;

/// Describes the plaintext value to wrap into a public handle, with its TEE type
/// and the resulting handle
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionOperation {
    pub value: String,
    pub tee_type: u8,
    pub handle: Handle,
}

/// Describes the 2 operand and 1 result handles for an arithmetic operation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArithmeticOperation {
    pub left_hand_operand: Handle,
    pub right_hand_operand: Handle,
    pub result: Handle,
}

/// Describes the 2 operand and 2 result handles for a safe arithmetic operation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SafeArithmeticOperation {
    pub left_hand_operand: Handle,
    pub right_hand_operand: Handle,
    pub success: Handle,
    pub result: Handle,
}

/// Describes the 2 operand and 1 result handles for a boolean comparison.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BooleanOperation {
    pub left_hand_operand: Handle,
    pub right_hand_operand: Handle,
    pub result: Handle,
}

/// Describes the 3 operand and 1 result handles for a select operation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectOperation {
    pub condition: Handle,
    pub if_true: Handle,
    pub if_false: Handle,
    pub result: Handle,
}

/// Describes the 3 operand and 3 result handles for a transfer operation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferOperation {
    pub balance_from: Handle,
    pub balance_to: Handle,
    pub amount: Handle,
    pub success: Handle,
    pub new_balance_from: Handle,
    pub new_balance_to: Handle,
}

/// Describes the 3 operand and 3 result handles for a mint operation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintOperation {
    pub balance_to: Handle,
    pub amount: Handle,
    pub total_supply: Handle,
    pub success: Handle,
    pub new_balance_to: Handle,
    pub new_total_supply: Handle,
}

/// Describes the 3 operand and 3 result handles for a burn operation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BurnOperation {
    pub balance_from: Handle,
    pub amount: Handle,
    pub total_supply: Handle,
    pub success: Handle,
    pub new_balance_from: Handle,
    pub new_total_supply: Handle,
}

/// Event payload with typed variants. The wire tag is the snake_case form of
/// the variant name (`add`, `safe_add`, `wrap_as_public_handle`, ...), see
/// [`Operator::wire_tag`] for the canonical mapping used when writing to the
/// `handles.operator` DB column.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Operator {
    WrapAsPublicHandle(EncryptionOperation),
    Add(ArithmeticOperation),
    Sub(ArithmeticOperation),
    Mul(ArithmeticOperation),
    Div(ArithmeticOperation),
    SafeAdd(SafeArithmeticOperation),
    SafeSub(SafeArithmeticOperation),
    SafeMul(SafeArithmeticOperation),
    SafeDiv(SafeArithmeticOperation),
    Eq(BooleanOperation),
    Ne(BooleanOperation),
    Ge(BooleanOperation),
    Gt(BooleanOperation),
    Le(BooleanOperation),
    Lt(BooleanOperation),
    Select(SelectOperation),
    Transfer(TransferOperation),
    Mint(MintOperation),
    Burn(BurnOperation),
}

impl Operator {
    /// Snake-case wire tag, the canonical string written to `handles.operator`.
    /// Matches `#[serde(rename_all = "snake_case", tag = "type")]` exactly.
    pub fn wire_tag(&self) -> &'static str {
        match self {
            Operator::WrapAsPublicHandle(_) => "wrap_as_public_handle",
            Operator::Add(_) => "add",
            Operator::Sub(_) => "sub",
            Operator::Mul(_) => "mul",
            Operator::Div(_) => "div",
            Operator::SafeAdd(_) => "safe_add",
            Operator::SafeSub(_) => "safe_sub",
            Operator::SafeMul(_) => "safe_mul",
            Operator::SafeDiv(_) => "safe_div",
            Operator::Eq(_) => "eq",
            Operator::Ne(_) => "ne",
            Operator::Ge(_) => "ge",
            Operator::Gt(_) => "gt",
            Operator::Le(_) => "le",
            Operator::Lt(_) => "lt",
            Operator::Select(_) => "select",
            Operator::Transfer(_) => "transfer",
            Operator::Mint(_) => "mint",
            Operator::Burn(_) => "burn",
        }
    }

    /// Handle ids this operator emits (writes), in deterministic order.
    ///
    /// These are the values persisted to `handles.handle_id`; operand and input
    /// handles are not included, and a single operator may emit several.
    pub fn emitted_handles(&self) -> Vec<&str> {
        match self {
            Operator::WrapAsPublicHandle(op) => vec![&op.handle],
            Operator::Add(op) | Operator::Sub(op) | Operator::Mul(op) | Operator::Div(op) => {
                vec![&op.result]
            }
            Operator::SafeAdd(op)
            | Operator::SafeSub(op)
            | Operator::SafeMul(op)
            | Operator::SafeDiv(op) => vec![&op.success, &op.result],
            Operator::Eq(op)
            | Operator::Ne(op)
            | Operator::Ge(op)
            | Operator::Gt(op)
            | Operator::Le(op)
            | Operator::Lt(op) => vec![&op.result],
            Operator::Select(op) => vec![&op.result],
            Operator::Transfer(op) => vec![&op.success, &op.new_balance_from, &op.new_balance_to],
            Operator::Mint(op) => vec![&op.success, &op.new_balance_to, &op.new_total_supply],
            Operator::Burn(op) => vec![&op.success, &op.new_balance_from, &op.new_total_supply],
        }
    }
}

/// Individual event within a transaction.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionEvent {
    pub log_index: u64,
    #[serde(flatten)]
    pub operator: Operator,
}

/// Message format grouping events by transaction. Published by `nox-ingestor`
/// to `nox_ingestor.<transaction_hash>`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionMessage {
    /// Chain ID where the events occurred (u32 on the wire; observer narrows
    /// to i32 at extract time per spec §2.D).
    pub chain_id: u32,
    /// Block number.
    pub block_number: u64,
    /// Caller address (top-level, applies to every event in the tx).
    pub caller: Address,
    /// Transaction hash.
    pub transaction_hash: String,
    /// Events in this transaction, ordered by `log_index`.
    pub events: Vec<TransactionEvent>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // TransactionMessage envelope fields
    const TEST_CHAIN_ID: u32 = 1;
    const TEST_BLOCK_NUMBER: u64 = 10;
    const TEST_CALLER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cfFFb92266";
    const TEST_TX_HASH: &str = "0xdead";

    // Operand handle bytes
    const HANDLE_L: &str = "0xaa";
    const HANDLE_R: &str = "0xbb";
    const HANDLE_RES: &str = "0xcc";
    const HANDLE_SUCCESS: &str = "0xdd";
    // Positional placeholders for *Operation variants with 4+ handle fields.
    const HANDLE_1: &str = "0x1";
    const HANDLE_2: &str = "0x2";
    const HANDLE_3: &str = "0x3";
    const HANDLE_4: &str = "0x4";
    const HANDLE_5: &str = "0x5";
    const HANDLE_6: &str = "0x6";

    fn make_arith() -> ArithmeticOperation {
        ArithmeticOperation {
            left_hand_operand: HANDLE_L.into(),
            right_hand_operand: HANDLE_R.into(),
            result: HANDLE_RES.into(),
        }
    }

    fn make_safe_arith() -> SafeArithmeticOperation {
        SafeArithmeticOperation {
            left_hand_operand: HANDLE_L.into(),
            right_hand_operand: HANDLE_R.into(),
            success: HANDLE_SUCCESS.into(),
            result: HANDLE_RES.into(),
        }
    }

    fn make_bool() -> BooleanOperation {
        BooleanOperation {
            left_hand_operand: HANDLE_L.into(),
            right_hand_operand: HANDLE_R.into(),
            result: HANDLE_RES.into(),
        }
    }

    /// Wrap an events-JSON-array fragment in a full `TransactionMessage`
    /// envelope. Centralises the chainId/blockNumber/caller/transactionHash
    /// boilerplate that every deserialize test repeats.
    fn make_tx_message_json(events_json: &str) -> String {
        format!(
            r#"{{
                "chainId": {TEST_CHAIN_ID},
                "blockNumber": {TEST_BLOCK_NUMBER},
                "caller": "{TEST_CALLER}",
                "transactionHash": "{TEST_TX_HASH}",
                "events": [{events_json}]
            }}"#
        )
    }

    fn parse(msg: &str) -> TransactionMessage {
        serde_json::from_str(msg).expect("payload must deserialize")
    }

    //  wire_tag for every variant

    #[test]
    fn wire_tag_returns_snake_case_string_when_called_on_any_variant() {
        assert_eq!(
            Operator::WrapAsPublicHandle(EncryptionOperation {
                value: "v".into(),
                tee_type: 1,
                handle: HANDLE_L.into()
            })
            .wire_tag(),
            "wrap_as_public_handle"
        );
        assert_eq!(Operator::Add(make_arith()).wire_tag(), "add");
        assert_eq!(Operator::Sub(make_arith()).wire_tag(), "sub");
        assert_eq!(Operator::Mul(make_arith()).wire_tag(), "mul");
        assert_eq!(Operator::Div(make_arith()).wire_tag(), "div");
        assert_eq!(Operator::SafeAdd(make_safe_arith()).wire_tag(), "safe_add");
        assert_eq!(Operator::SafeSub(make_safe_arith()).wire_tag(), "safe_sub");
        assert_eq!(Operator::SafeMul(make_safe_arith()).wire_tag(), "safe_mul");
        assert_eq!(Operator::SafeDiv(make_safe_arith()).wire_tag(), "safe_div");
        assert_eq!(Operator::Eq(make_bool()).wire_tag(), "eq");
        assert_eq!(Operator::Ne(make_bool()).wire_tag(), "ne");
        assert_eq!(Operator::Ge(make_bool()).wire_tag(), "ge");
        assert_eq!(Operator::Gt(make_bool()).wire_tag(), "gt");
        assert_eq!(Operator::Le(make_bool()).wire_tag(), "le");
        assert_eq!(Operator::Lt(make_bool()).wire_tag(), "lt");
        assert_eq!(
            Operator::Select(SelectOperation {
                condition: HANDLE_1.into(),
                if_true: HANDLE_2.into(),
                if_false: HANDLE_3.into(),
                result: HANDLE_4.into()
            })
            .wire_tag(),
            "select"
        );
        assert_eq!(
            Operator::Transfer(TransferOperation {
                balance_from: HANDLE_1.into(),
                balance_to: HANDLE_2.into(),
                amount: HANDLE_3.into(),
                success: HANDLE_4.into(),
                new_balance_from: HANDLE_5.into(),
                new_balance_to: HANDLE_6.into()
            })
            .wire_tag(),
            "transfer"
        );
        assert_eq!(
            Operator::Mint(MintOperation {
                balance_to: HANDLE_1.into(),
                amount: HANDLE_2.into(),
                total_supply: HANDLE_3.into(),
                success: HANDLE_4.into(),
                new_balance_to: HANDLE_5.into(),
                new_total_supply: HANDLE_6.into()
            })
            .wire_tag(),
            "mint"
        );
        assert_eq!(
            Operator::Burn(BurnOperation {
                balance_from: HANDLE_1.into(),
                amount: HANDLE_2.into(),
                total_supply: HANDLE_3.into(),
                success: HANDLE_4.into(),
                new_balance_from: HANDLE_5.into(),
                new_total_supply: HANDLE_6.into()
            })
            .wire_tag(),
            "burn"
        );
    }

    //  emitted_handles

    #[test]
    fn emitted_handles_returns_result_only_when_operator_is_arithmetic() {
        assert_eq!(
            Operator::Add(make_arith()).emitted_handles(),
            vec![HANDLE_RES]
        );
    }

    #[test]
    fn emitted_handles_returns_success_then_result_when_operator_is_safe_arithmetic() {
        assert_eq!(
            Operator::SafeAdd(make_safe_arith()).emitted_handles(),
            vec![HANDLE_SUCCESS, HANDLE_RES]
        );
    }

    #[test]
    fn emitted_handles_returns_three_balances_in_order_when_operator_is_transfer() {
        let op = Operator::Transfer(TransferOperation {
            balance_from: HANDLE_1.into(),
            balance_to: HANDLE_2.into(),
            amount: HANDLE_3.into(),
            success: HANDLE_4.into(),
            new_balance_from: HANDLE_5.into(),
            new_balance_to: HANDLE_6.into(),
        });
        assert_eq!(op.emitted_handles(), vec![HANDLE_4, HANDLE_5, HANDLE_6]);
    }

    //  Deserialize

    #[test]
    fn deserialize_returns_wrap_as_public_handle_variant_when_payload_type_is_wrap_as_public_handle()
     {
        let json = make_tx_message_json(
            r#"{
                "logIndex": 0,
                "type": "wrap_as_public_handle",
                "value": "42",
                "teeType": 1,
                "handle": "0xaaaa"
            }"#,
        );
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::WrapAsPublicHandle(op) => {
                assert_eq!(op.value, "42");
                assert_eq!(op.tee_type, 1);
                assert_eq!(op.handle, "0xaaaa");
            }
            other => panic!("expected WrapAsPublicHandle, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_returns_add_variant_when_payload_type_is_add() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "add",
                 "leftHandOperand": "{HANDLE_L}", "rightHandOperand": "{HANDLE_R}", "result": "{HANDLE_RES}"}}"#
        ));
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::Add(op) => assert_eq!(op.result, HANDLE_RES),
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_returns_safe_add_variant_when_payload_type_is_safe_add() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "safe_add",
                 "leftHandOperand": "{HANDLE_L}", "rightHandOperand": "{HANDLE_R}",
                 "success": "{HANDLE_SUCCESS}", "result": "{HANDLE_RES}"}}"#
        ));
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::SafeAdd(op) => {
                assert_eq!(op.success, HANDLE_SUCCESS);
                assert_eq!(op.result, HANDLE_RES);
            }
            other => panic!("expected SafeAdd, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_returns_eq_variant_when_payload_type_is_eq() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "eq",
                 "leftHandOperand": "{HANDLE_L}", "rightHandOperand": "{HANDLE_R}", "result": "{HANDLE_RES}"}}"#
        ));
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::Eq(op) => assert_eq!(op.result, HANDLE_RES),
            other => panic!("expected Eq, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_returns_select_variant_when_payload_type_is_select() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "select",
                 "condition": "{HANDLE_1}", "ifTrue": "{HANDLE_2}",
                 "ifFalse": "{HANDLE_3}", "result": "{HANDLE_4}"}}"#
        ));
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::Select(op) => assert_eq!(op.result, HANDLE_4),
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_returns_transfer_variant_when_payload_type_is_transfer() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "transfer",
                 "balanceFrom": "{HANDLE_1}", "balanceTo": "{HANDLE_2}", "amount": "{HANDLE_3}",
                 "success": "{HANDLE_4}", "newBalanceFrom": "{HANDLE_5}", "newBalanceTo": "{HANDLE_6}"}}"#
        ));
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::Transfer(op) => {
                assert_eq!(op.success, HANDLE_4);
                assert_eq!(op.new_balance_from, HANDLE_5);
                assert_eq!(op.new_balance_to, HANDLE_6);
            }
            other => panic!("expected Transfer, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_returns_mint_variant_when_payload_type_is_mint() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "mint",
                 "balanceTo": "{HANDLE_1}", "amount": "{HANDLE_2}", "totalSupply": "{HANDLE_3}",
                 "success": "{HANDLE_4}", "newBalanceTo": "{HANDLE_5}", "newTotalSupply": "{HANDLE_6}"}}"#
        ));
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::Mint(op) => {
                assert_eq!(op.success, HANDLE_4);
                assert_eq!(op.new_balance_to, HANDLE_5);
                assert_eq!(op.new_total_supply, HANDLE_6);
            }
            other => panic!("expected Mint, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_returns_burn_variant_when_payload_type_is_burn() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "burn",
                 "balanceFrom": "{HANDLE_1}", "amount": "{HANDLE_2}", "totalSupply": "{HANDLE_3}",
                 "success": "{HANDLE_4}", "newBalanceFrom": "{HANDLE_5}", "newTotalSupply": "{HANDLE_6}"}}"#
        ));
        let m = parse(&json);
        match &m.events[0].operator {
            Operator::Burn(op) => {
                assert_eq!(op.success, HANDLE_4);
                assert_eq!(op.new_balance_from, HANDLE_5);
                assert_eq!(op.new_total_supply, HANDLE_6);
            }
            other => panic!("expected Burn, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_preserves_payload_array_order_when_tx_has_multiple_events() {
        let json = make_tx_message_json(&format!(
            r#"{{"logIndex": 0, "type": "wrap_as_public_handle", "value":"1","teeType":1,"handle":"0xa"}},
               {{"logIndex": 1, "type": "add", "leftHandOperand":"{HANDLE_1}","rightHandOperand":"{HANDLE_2}","result":"{HANDLE_3}"}},
               {{"logIndex": 2, "type": "safe_add", "leftHandOperand":"{HANDLE_1}","rightHandOperand":"{HANDLE_2}","success":"{HANDLE_4}","result":"{HANDLE_5}"}}"#
        ));
        let m = parse(&json);
        assert_eq!(m.events.len(), 3);
        assert_eq!(m.events[0].operator.wire_tag(), "wrap_as_public_handle");
        assert_eq!(m.events[1].operator.wire_tag(), "add");
        assert_eq!(m.events[2].operator.wire_tag(), "safe_add");
    }
}
