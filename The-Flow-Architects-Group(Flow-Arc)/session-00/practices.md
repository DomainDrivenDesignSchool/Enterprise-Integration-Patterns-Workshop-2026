# 🟠 Group 2 - The Flow Architects

> **Messages don't create value. Messages moving together do.**

---

# Mission

Group 1 discovered **what** moves through the enterprise.

Your responsibility is to discover **how** those messages move.

You are not designing software.

You are not designing architecture.

You are discovering the behavior of information.

Your final artifact will become the foundation for the City Planners.

---

# Input

Use the **Enterprise Message Catalog** created by Group 1.

Do not invent new messages unless absolutely necessary.

---

# Task 1 - Build the Enterprise Message Flow

Arrange the messages into a complete business flow.

Focus on the movement of information.

Ask yourselves:

- Which message starts the flow?
- Which message causes another?
- Which messages can happen in parallel?
- Which messages must wait?
- Where does the flow finish?

Draw a single end-to-end flow.

---

# Task 2 — Observe the Flow

Walk through your message flow.

Simply observe what happens.

Do you notice places where...

- One message becomes many?
- Many messages eventually become one?
- A message seems to choose between different paths?
- The same message is sent to multiple receivers?
- Some messages disappear because nobody needs them?
- Messages arrive in an unexpected order?
- A message waits for another before continuing?

Don't try to solve these situations.

Just mark them on your diagram.

We'll give them names during the workshop!(WARNING, spoling )

Write down your reasoning.

There are no correct answers yet.

---

# Task 3 - Find Flow Problems

Walk through your flow.

Mark every place where you observe one of the following situations.

## Waiting

A message cannot continue.

---

## Duplication

The same information appears more than once.

---

## Fan-Out

One message creates several independent messages.

---

## Fan-In

Multiple messages must arrive before work can continue.

---

## Reordering

Would the business still work if two messages arrived in a different order?

---

## Missing Messages

If one message disappeared...

What would happen?

---

## External Dependency

Does the flow depend on another organization or external system?

---

Document every interesting observation.

Do not solve the problems.

---

# Task 4 - Design Review

Review your own flow.

Discuss the following questions.

- Which part of the flow feels fragile?
- Which part looks unnecessarily complex?
- Which messages seem to know too much?
- Which messages seem to travel too far?
- Which part would become difficult to scale?
- If your business doubled tomorrow, where would the first bottleneck appear?

Again...

Don't solve anything.

Just observe.

---

# Final Deliverable

Produce one artifact.

# Enterprise Message Flow

Your diagram should answer one question:

> **How does business information move through the enterprise?**

Bring all assumptions, disagreements and questions to Session 1.

We'll revisit every one of them together.