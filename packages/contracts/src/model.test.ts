import { describe, expect, it } from "vite-plus/test";

import { ProviderDriverKind } from "./providerInstance.ts";
import {
  ANTIGRAVITY_DEFAULT_MODEL,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_TEXT_GENERATION_MODEL_BY_PROVIDER,
  MODEL_SLUG_ALIASES_BY_PROVIDER,
  PI_DEFAULT_MODEL,
  PROVIDER_DISPLAY_NAMES,
} from "./model.ts";

const PI = ProviderDriverKind.make("pi");

describe("Pi provider defaults", () => {
  it("resolves fresh Pi threads to a Pi-native sentinel instead of the Codex slug", () => {
    expect(DEFAULT_MODEL_BY_PROVIDER[PI]).toBe(PI_DEFAULT_MODEL);
    expect(DEFAULT_MODEL_BY_PROVIDER[PI]).not.toBe(DEFAULT_MODEL);
  });

  it("keeps the sentinel distinct from other providers' sentinels", () => {
    expect(PI_DEFAULT_MODEL).not.toBe(ANTIGRAVITY_DEFAULT_MODEL);
    expect(PI_DEFAULT_MODEL).not.toBe(DEFAULT_MODEL);
  });

  it("carries no provider/model separator so the adapter can treat it as keep-current", () => {
    // PiAdapter applies a model selection by splitting on `/`; a bare
    // sentinel must never form a `provider/modelId` pair that would force a
    // concrete model onto the session.
    expect(PI_DEFAULT_MODEL.includes("/")).toBe(false);
    expect(PI_DEFAULT_MODEL.trim()).toBe(PI_DEFAULT_MODEL);
  });

  it("exposes a display name for the Pi provider", () => {
    expect(PROVIDER_DISPLAY_NAMES[PI]).toBe("Pi");
  });

  it("does not add a Pi text-generation default", () => {
    // Pi auxiliary generation is explicitly unsupported (`supportsTextGeneration:
    // false`); a text-generation fallback must never target Pi.
    expect(DEFAULT_TEXT_GENERATION_MODEL_BY_PROVIDER[PI]).toBeUndefined();
  });

  it("does not alias the sentinel into another model slug", () => {
    expect(MODEL_SLUG_ALIASES_BY_PROVIDER[PI]).toBeUndefined();
  });
});
