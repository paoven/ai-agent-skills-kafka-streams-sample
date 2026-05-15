# Prompts Used to Build This Project

This project was built entirely through conversation with [Claude Code](https://claude.ai/claude-code) and the [Confluent Agent Skills](https://github.com/confluentinc/agent-skills) plugin. Below are the actual prompts used, in order.

---

**1.** do you know a kafka streams programming skill?

**2.** bootstrap a kafka stream apps which joins Customer events from customers topic (read as k global table) with events coming from orders topic which can contain events of different types (order created, order line added, order line removed, order payed). Join the customers kglobal table in papi with orders but emit an aggregation with customer info and total order amount and full list of items only when event order closed arrives. Add unit tests, use topology test drivers, mocks of sr as provided by kafka streams

**3.** run tests

**4.** docker compose up -d

**5.** add a README to the project which explains this is a vibe coding experiment by leveraging claude and this new agent skills to build a sample kafka streams application https://github.com/confluentinc/agent-skills/blob/main/README.md In the README add also instructions to build, run project tests, run project. Add also sample commands to create from terminal proper events that simulate an order creation for a customer and observing final aggregate emission on final destination topic only when ordered is closed

**7.** are there any secrets in this repo?

**8.** add a proper .gitignore

**9.** create a proper license which does not compromise me or the company

**11.** add used prompts in a proper file so other people can see them