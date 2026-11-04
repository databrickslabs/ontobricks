from back.core.reasoning.swrl.SWRLFlatCypherTranslator import SWRLFlatCypherTranslator


def test_translates_customer_contract_meter_rule_to_read_only_cypher():
    query = SWRLFlatCypherTranslator("sandbox").build_inference_sql(
        "sandbox",
        {
            "antecedent": (
                "Customer(?customer) ∧ hasContract(?customer, ?contract) "
                "∧ hasMeter(?contract, ?meter)"
            ),
            "consequent": "owns(?customer, ?meter)",
            "uri_map": {
                "customer": "https://example.test#Customer",
                "hascontract": "https://example.test/hasContract",
                "hasmeter": "https://example.test/hasMeter",
                "owns": "https://example.test/owns",
            },
        },
    )

    assert query.startswith("MATCH ")
    assert "CREATE" not in query
    assert "RETURN DISTINCT v_customer.uri AS subject" in query
    assert "'https://example.test/owns' AS predicate" in query

