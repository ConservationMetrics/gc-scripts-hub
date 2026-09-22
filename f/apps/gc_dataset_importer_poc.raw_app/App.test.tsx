import "@testing-library/jest-dom/vitest";

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import { backend } from "./wmill";

vi.mock("./wmill", () => ({
  backend: {
    apply_import: vi.fn(),
    check_dataset_name: vi.fn(),
    list_datasets: vi.fn(),
    preview_import: vi.fn(),
    stage_import: vi.fn(),
  },
}));

const mockedBackend = vi.mocked(backend);

const stagedImport = {
  fields: ["code", "note"],
  import_id: "import-1",
  record_count: 1,
  source_format: "csv",
};

const preview = {
  added: 1,
  columns_added: 0,
  deleted: 0,
  final_count: 2,
  preview_id: "preview-1",
  unchanged: 1,
  updated: 0,
};

async function chooseExistingGoal(goal: "Append" | "Merge" | "Sync") {
  fireEvent.click(screen.getByRole("radio", { name: new RegExp(goal) }));
  fireEvent.click(screen.getByRole("button", { name: "Next" }));
  fireEvent.change(screen.getByLabelText("Dataset"), {
    target: { value: "observations" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Next" }));
  fireEvent.change(screen.getByLabelText(/Drop a file here/), {
    target: { files: [new File(["code,note\nA,new\n"], "rows.csv")] },
  });
  fireEvent.click(screen.getByRole("button", { name: "Stage upload" }));
  await waitFor(() => expect(mockedBackend.stage_import).toHaveBeenCalled());
}

afterEach(cleanup);

describe("Dataset importer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedBackend.list_datasets.mockResolvedValue(["observations"]);
    mockedBackend.stage_import.mockResolvedValue(stagedImport);
    mockedBackend.preview_import.mockResolvedValue(preview);
    mockedBackend.apply_import.mockResolvedValue({ success: true });
  });

  it("uses the conditional identity journey", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());

    fireEvent.click(screen.getByRole("radio", { name: /Merge/ }));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(
      screen.getByRole("option", { name: "observations" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Identity")).toBeInTheDocument();
  });

  it("omits identity for append", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());

    fireEvent.click(screen.getByRole("radio", { name: /Append/ }));
    expect(screen.queryByText("Identity")).not.toBeInTheDocument();
  });

  it("rejects an oversized file before staging", async () => {
    mockedBackend.check_dataset_name.mockResolvedValue({
      available: true,
      table_name: "birds",
    });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.change(screen.getByLabelText("Dataset name"), {
      target: { value: "Birds" },
    });
    await waitFor(() =>
      expect(mockedBackend.check_dataset_name).toHaveBeenCalled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Next" }));

    const file = new File([new Uint8Array(25 * 1024 * 1024 + 1)], "large.csv");
    fireEvent.change(screen.getByLabelText(/Drop a file here/), {
      target: { files: [file] },
    });
    expect(screen.getByRole("alert")).toHaveTextContent("25 MiB");
    expect(mockedBackend.stage_import).not.toHaveBeenCalled();
  });

  it("stages, previews, and confirms an append", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Append");

    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await waitFor(() =>
      expect(mockedBackend.preview_import).toHaveBeenCalledWith({
        identity_fields: [],
        import_id: "import-1",
        update_policy: "imported",
      }),
    );
    expect(screen.getByText("Rows added").previousSibling).toHaveTextContent(
      "1",
    );

    fireEvent.click(screen.getByRole("button", { name: "Confirm import" }));
    await waitFor(() =>
      expect(mockedBackend.apply_import).toHaveBeenCalledWith({
        import_id: "import-1",
        preview_id: "preview-1",
      }),
    );
    expect(screen.getByRole("status")).toHaveTextContent(
      "Import applied successfully",
    );
  });

  it("requires identity and confirms destructive Sync explicitly", async () => {
    const destructivePreview = { ...preview, deleted: 3, final_count: 1 };
    mockedBackend.preview_import.mockResolvedValue(destructivePreview);
    const confirmation = vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Sync");

    expect(screen.getByLabelText("Identity field 2")).toBeDisabled();
    fireEvent.change(screen.getByLabelText("Identity field 1"), {
      target: { value: "code" },
    });
    expect(screen.getByLabelText("Identity field 2")).toBeEnabled();
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByText(/permanently delete 3 unmatched records/);
    fireEvent.click(screen.getByRole("button", { name: "Confirm import" }));

    expect(confirmation).toHaveBeenCalledWith(
      "Sync will delete 3 records from observations. Continue?",
    );
    await waitFor(() =>
      expect(mockedBackend.apply_import).toHaveBeenCalledWith({
        import_id: "import-1",
        preview_id: "preview-1",
      }),
    );
    confirmation.mockRestore();
  });

  it("forces a new preview after a stale confirmation", async () => {
    mockedBackend.apply_import.mockRejectedValue(
      new Error("The target changed. Review the import again."),
    );
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Append");
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByRole("button", { name: "Confirm import" });
    fireEvent.click(screen.getByRole("button", { name: "Confirm import" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Review the import again",
    );
    expect(
      screen.getByRole("button", { name: "Preview changes" }),
    ).toBeInTheDocument();
  });
});
