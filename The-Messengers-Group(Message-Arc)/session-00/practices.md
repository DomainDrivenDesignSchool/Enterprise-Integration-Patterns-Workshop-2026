# 🔵 Group 1 - The Messengers

> Great integrations begin with well-designed messages.

---

# Mission

Your responsibility is to discover **what actually moves between systems**.

Not APIs.

Not databases.

Not technologies.

Messages.

Your final deliverable will become the foundation for the other groups.

---

# Business Process

Select ONE real business process.

Examples:

- Order Processing
- Customer Registration
- Payment
- Shipment
- Insurance Claim
- Loan Approval

Use the same process throughout all assignments.

---

# Task 1 - Build the Message Catalog

Identify every message exchanged during the process.

For every message complete the following table.

| Message | Type | Producer | Consumer | Business Meaning |
|----------|------|----------|----------|------------------|

Type must be one of:

- Command
- Event
- Document
- Query

Do not overthink.

Document your reasoning.

---
### A Small Challenge 😈

Yes, we know.

We haven't taught message types yet.

And yes...

We're still asking you to classify them.

Welcome to the workshop. 😄

Make your best guess.

Argue.

Disagree.

Defend your reasoning.

Don't Google it.

Don't open the Enterprise Integration Patterns book.

Just use your experience.

We'll come back to this exercise in Session 1 and see how many assumptions survive.
---

# Task 2 - Validate the Catalog

Review every message.

Ask the following questions.

- Does this message represent a business fact?
- Could another team understand its name?
- Does it carry business intent?
- Is it independent from implementation?

Document disagreements.

---

# Task 3 - Find Bad Messages

Identify three examples of poor message design.

Examples:

- Generic events
- Huge payloads
- Shared DTOs
- Ambiguous names
- Technology-driven messages

For every example answer:

- Why is it problematic?
- What production issues has it caused?

Do not redesign it.

---
# Task 4 - Message Anatomy

Now pick five messages from your catalog.

For each one, discuss the following questions.

There are no correct answers yet.

The goal is to challenge your assumptions.

---

### Identity

What makes this message unique?

Is it its name?

Its purpose?

Its payload?

Something else?

---

### Outside vs Inside

Imagine someone receives only the message envelope.

What can they understand without opening it?

What information belongs outside?

What information belongs inside?

---

### Business Meaning

If you removed the payload and only kept the message name...

Would the business still understand what happened?

Why?

---

### State

Does this message describe:

- something that happened?
- something that should happen?
- the current state of something?
- only part of a state?

Could another system rebuild the business state from this message alone?

---

### Evolution

If the business changes next month...

Which part is more likely to change?

- the message name?
- the payload?
- both?

Why?

---

Write down every disagreement.

We'll revisit these questions during Session 1.

---

# Final Deliverable

Produce ONE artifact.

## Enterprise Message Catalog

This document will be used by the Flow Architects during Session 1.