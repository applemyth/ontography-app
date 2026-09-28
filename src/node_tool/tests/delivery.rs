use super::*;

#[tokio::test]
async fn failed_turns_use_existing_retry_policy_and_do_not_retry_committed_work() {
    let fixture = Fixture::new(
        document(json!({"retry":{"max_attempts":2,"initial_delay_secs":0,"max_delay_secs":0}})),
        "worker",
        None,
    )
    .await;
    fixture.deliver("retry me").await;
    let reply = fixture.tools.next_message().await.unwrap().unwrap();
    let attempt = reply.value().unwrap()["attempt_id"]
        .as_str()
        .unwrap()
        .to_owned();
    reply.sent().await.unwrap();
    fixture
        .tools
        .message_failed(&attempt, "model connection failed")
        .await
        .unwrap();
    let retried = fixture.tools.next_message().await.unwrap().unwrap();
    let next = retried.value().unwrap();
    assert_ne!(next["attempt_id"], attempt);
    retried.sent().await.unwrap();
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id":next["attempt_id"],"result":{"message":"done"}}),
        )
        .await;
    fixture
        .tools
        .message_failed(next["attempt_id"].as_str().unwrap(), "late error")
        .await
        .unwrap();
    assert!(fixture.tools.next_message().await.unwrap().is_none());
    assert_eq!(fixture.pending("sink").await, ["done"]);
    fixture.stop().await;
}

#[tokio::test]
async fn incoming_conversation_input_is_claimed_recorded_and_can_be_replied_to() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    assert!(fixture.tools.next_message().await.unwrap().is_none());
    fixture
        .deliver("Review the parser.\nKeep the existing API.")
        .await;
    let reply = fixture.tools.next_message().await.unwrap().unwrap();
    let value = reply.value().unwrap();
    assert_eq!(value["type"], "ontography_message");
    assert_eq!(value["inputs"][0]["from"], "source");
    assert_eq!(
        value["inputs"][0]["message"],
        "Review the parser.\nKeep the existing API."
    );
    assert!(value["attempt_id"].is_string());
    assert!(
        fixture.tools.next_message().await.unwrap().is_none(),
        "the same work must not be injected twice"
    );
    let (invocation, sequence) = reply.receipt().unwrap();
    let digest = ContentDigest::compute(reply.bytes());
    let receipt = fixture
        .session
        .invocation_events(invocation, 0, 100)
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.sequence == sequence)
        .unwrap();
    assert_eq!(receipt.content_digest, digest);
    assert_eq!(receipt.state, ReceiptState::Prepared);
    reply.sent().await.unwrap();
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id":value["attempt_id"],"result":{"message":"Reviewed."}}),
        )
        .await;
    assert_eq!(fixture.pending("sink").await, ["Reviewed."]);
    fixture.stop().await;
}

#[tokio::test]
async fn delivery_is_independent_of_tool_selection_and_bounds_open_attempts() {
    let fixture = Fixture::new(document(json!({"tools":[]})), "worker", None).await;
    for index in 0..10 {
        fixture.deliver(&format!("Message {index}")).await;
    }
    for _ in 0..8 {
        let reply = fixture.tools.next_message().await.unwrap().unwrap();
        assert!(reply.value().unwrap()["inputs"][0]["message"].is_string());
        reply.sent().await.unwrap();
    }
    assert!(fixture.tools.next_message().await.unwrap().is_none());
    assert!(fixture.tools.catalog().is_empty());
    fixture.stop().await;
}

#[tokio::test]
async fn initial_and_large_messages_keep_task_identity_and_input_handles() {
    let fixture = Fixture::new(entry_document(json!([])), "worker", Some("First task")).await;
    let reply = fixture.tools.next_message().await.unwrap().unwrap();
    let input = reply.value().unwrap();
    assert_eq!(input["initial"], true);
    assert_eq!(input["inputs"][0]["message"], "First task");
    assert!(input["inputs"][0]["from"].is_null());
    drop(reply); // Loss leaves the receipt prepared and the task claimed.
    assert!(fixture.tools.next_message().await.unwrap().is_none());
    fixture.stop().await;

    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver(&"a".repeat(40_000)).await;
    let reply = fixture.tools.next_message().await.unwrap().unwrap();
    let input = reply.value().unwrap();
    assert!(input["inputs"][0]["message_omitted"].is_string());
    assert!(input["inputs"][0]["handle"].is_string());
    assert!(reply.bytes().len() < 2000);
    reply.sent().await.unwrap();
    let read = fixture
        .ok(
            "read_package",
            json!({"attempt_id":input["attempt_id"],"handle":input["inputs"][0]["handle"]}),
        )
        .await;
    assert!(read["text"].as_str().unwrap().contains(&"a".repeat(40_000)));
    fixture.stop().await;
}
