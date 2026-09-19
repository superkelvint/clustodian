# Transitions

This directory contains the different representations of a state transition.
They are similar by design, but they belong to different points in the
controller-to-participant flow.

```text
TransitionRequest    controller's semantic one-step decision
PendingTransition    semantic decision already in flight during selection
TransitionMessage    serialized queue message delivered to a participant
```

## Why there are three types

`TransitionRequest` knows logical resource, partition, instance, source state,
and target state. It does not know sessions or wire formats.

`PendingTransition` is the smaller form used by message selection and
throttling. It deliberately omits resource identity and message identity
because those stages already operate within a resource and only need endpoint
states.

`TransitionMessage` is the production JSON shape. Its string fields and
serialized names are part of the coordination contract. The participant
validates those strings into typed model values before invoking the application
handler.

## Reading the code

- Read `request.rs` for the controller-side semantic value.
- Read `pending.rs` to see what the M5/M6 algorithms need to reserve.
- Read `message.rs` last for the wire representation and deterministic legacy
  message-ID generation.

The participant is responsible for checking the message type, target session,
current source state, and legal next hop before executing it. The application
handler receives the typed `TransitionExecution` from the participant module,
not the raw wire message.
