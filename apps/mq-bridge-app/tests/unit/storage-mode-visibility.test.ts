// @vitest-environment jsdom

import { describe, expect, test } from "vitest";
import { buildSettingsSchema } from "../../ui/src/lib/settings";

describe("storage mode visibility", () => {
  test("describes unsupported current mode in cli settings", () => {
    const schema = buildSettingsSchema(
      {
        properties: {
          config_security: {
            type: "object",
            properties: {
              mode: {
                type: "string",
                enum: ["unencrypted", "balanced", "env_temporary_messages", "temporary_messages", "sensitive", "durable"],
              },
            },
          },
        },
      },
      {
        target: "cli",
        encrypted: false,
        persistent: true,
        keySource: "env",
        keyStoreAvailable: false,
        encryptedConfigAvailable: false,
        persistentMessagesAvailable: false,
        configEncrypted: false,
        messagesEncrypted: false,
        messagesPersistent: true,
      },
      {
        config_security: { mode: "durable" },
      },
    );

    expect((schema.properties.config_security as any).properties.mode.description).toContain(
      "durable (Current, unsupported here): Unavailable.",
    );
  });
});
