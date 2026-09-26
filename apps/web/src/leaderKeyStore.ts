import { create } from "zustand";

interface LeaderKeyStore {
  /** Keys typed since Space, or null with no sequence in flight. */
  readonly pending: string | null;
  readonly setPending: (pending: string | null) => void;
}

/** Leader sequence state shared with surfaces that react to it, such as the sidebar's 1–9 badges. */
export const useLeaderKeyStore = create<LeaderKeyStore>((set) => ({
  pending: null,
  setPending: (pending) => set({ pending }),
}));

/** Space was pressed and nothing has followed yet: 1–9 would open a thread, like a held ⌘. */
export const selectLeaderAwaitingKey = (state: LeaderKeyStore): boolean => state.pending === "";
