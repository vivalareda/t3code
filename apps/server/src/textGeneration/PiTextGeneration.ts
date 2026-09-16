/**
 * Pi text generation — unsupported by design.
 *
 * A normal Pi coding session boots every extension and tool, so an automatic
 * title or commit prompt could launch subagents or otherwise act on the
 * workspace. T3 reports these auxiliary operations as unsupported instead of
 * risking that path; `supportsTextGeneration: false` on the snapshot keeps
 * clients from offering them.
 *
 * @module textGeneration/PiTextGeneration
 */
import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { TextGeneration } from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  new TextGenerationError({
    operation,
    detail:
      "Pi does not support T3 Code's automatic text generation. Run prompts directly in a Pi thread instead.",
  });

export const makePiTextGeneration = Effect.succeed({
  generateCommitMessage: () => Effect.fail(unsupported("generateCommitMessage")),
  generatePrContent: () => Effect.fail(unsupported("generatePrContent")),
  generateBranchName: () => Effect.fail(unsupported("generateBranchName")),
  generateThreadTitle: () => Effect.fail(unsupported("generateThreadTitle")),
} satisfies TextGeneration["Service"]);
