import {
  DEFAULT_MODEL,
  PI_DEFAULT_MODEL,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { getDefaultServerModel, getProviderModelCapabilities } from "./providerModels";

const PROVIDER = ProviderDriverKind.make("claudeAgent");

function capabilities(id: string): ModelCapabilities {
  return {
    optionDescriptors: [{ id, label: id, type: "boolean" }],
  };
}

function model(input: {
  slug: string;
  capabilities: ModelCapabilities;
  aliases?: ReadonlyArray<string>;
  isCustom?: boolean;
}): ServerProviderModel {
  return {
    slug: input.slug,
    name: input.slug,
    ...(input.aliases ? { aliases: [...input.aliases] } : {}),
    isCustom: input.isCustom ?? false,
    capabilities: input.capabilities,
  };
}

function piProvider(models: ReadonlyArray<ServerProviderModel> = []): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make("pi"),
    driver: ProviderDriverKind.make("pi"),
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "unknown" },
    checkedAt: "2026-01-01T00:00:00.000Z",
    models,
    slashCommands: [],
    skills: [],
  };
}

describe("getDefaultServerModel", () => {
  it("resolves a Pi provider with no catalog to the Pi sentinel, not the Codex slug", () => {
    expect(getDefaultServerModel([piProvider()], ProviderDriverKind.make("pi"))).toBe(
      PI_DEFAULT_MODEL,
    );
    expect(getDefaultServerModel([piProvider()], ProviderDriverKind.make("pi"))).not.toBe(
      DEFAULT_MODEL,
    );
  });

  it("prefers a concrete discovered Pi model over the sentinel", () => {
    const discovered = model({
      slug: "anthropic/claude-opus-4",
      capabilities: {},
    });
    expect(getDefaultServerModel([piProvider([discovered])], ProviderDriverKind.make("pi"))).toBe(
      "anthropic/claude-opus-4",
    );
  });
});

describe("getProviderModelCapabilities", () => {
  it("resolves model-declared aliases", () => {
    const aliasCapabilities = capabilities("aliased-option");
    const models = [
      model({
        slug: "synthetic-model",
        aliases: ["Legacy-Synthetic-Model"],
        capabilities: aliasCapabilities,
      }),
    ];

    expect(getProviderModelCapabilities(models, "legacy-synthetic-model", PROVIDER)).toEqual(
      aliasCapabilities,
    );
  });

  it("prefers an exact custom slug over a built-in model alias", () => {
    const customCapabilities = capabilities("custom-option");
    const models = [
      model({
        slug: "synthetic-model",
        aliases: ["custom-model"],
        capabilities: capabilities("built-in-option"),
      }),
      model({ slug: "custom-model", capabilities: customCapabilities, isCustom: true }),
    ];

    expect(getProviderModelCapabilities(models, " custom-model ", PROVIDER)).toEqual(
      customCapabilities,
    );
  });

  it("returns empty capabilities for an unknown slug", () => {
    const models = [
      model({
        slug: "default-model",
        capabilities: capabilities("default-option"),
      }),
    ];

    expect(getProviderModelCapabilities(models, "unknown-model", PROVIDER)).toEqual({
      optionDescriptors: [],
    });
  });
});
