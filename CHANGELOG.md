# Changelog

## 0.1.0 (2026-08-03)

### Features

* **cloudevents:** add CloudEvents 1.0 envelope binding and validation ([3e23cc0](https://github.com/camcima/werken/commit/3e23cc04203cbb66984b971feb520645058a95fa))
* **pubsub:** add EventPublisher ([9eff651](https://github.com/camcima/werken/commit/9eff651cba2429ee3c2bc6fe44f57dba2800ce4e))
* **pubsub:** add lifecycle, flow control and drain on shutdown ([c14d7be](https://github.com/camcima/werken/commit/c14d7be2defdda3dce23deecc5d41f00251731d2))
* **pubsub:** add Nest transport with schema, dead-lettering and idempotency ([565ce86](https://github.com/camcima/werken/commit/565ce86cd146e03a5551330a5d7a5babceb21da4))
* **pubsub:** add resource prefixing for shared dev projects ([18fd217](https://github.com/camcima/werken/commit/18fd2171f2778c9e776cd5631fce60741b7cec78))
* **pubsub:** add tracing, metrics and structured logging ([b4d4b03](https://github.com/camcima/werken/commit/b4d4b0339ba8930dfe9f80e5a63308cb5bb099b9))
* **pubsub:** add wildcard routing, worked example and migration guide ([761a21c](https://github.com/camcima/werken/commit/761a21ce84a8f6d41ba0382403a99d5a56f9b6fc))

### Bug Fixes

* **docs:** centre the logo by hugging its viewBox to the artwork ([9c21672](https://github.com/camcima/werken/commit/9c2167203d6552921432b9755ede5092fdeb737c))
* **pubsub:** close out low-severity findings and documentation parity ([87c8778](https://github.com/camcima/werken/commit/87c87780b8f39258b8f3549a4ceb51fef76d2d46))
* **pubsub:** emit the schema cache metric and close untested paths ([1d92f29](https://github.com/camcima/werken/commit/1d92f298ac5c3c25d446ca1f338e1859a0dafe1e))
* **pubsub:** harden shutdown, bound the router cache and report partial batch failures ([3013fb7](https://github.com/camcima/werken/commit/3013fb74e82129439b545dc0db0a80f3fa61c432))
* **pubsub:** wire up telemetry and stop the harness dropping events ([57e5ece](https://github.com/camcima/werken/commit/57e5eceb1d39c3928b7e038c7fde4caec07293cb))
