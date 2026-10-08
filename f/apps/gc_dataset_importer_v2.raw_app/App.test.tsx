import "@testing-library/jest-dom/vitest";

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
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
  eligible_identity_fields: ["code", "note"],
  fields: ["code", "note"],
  source_mapping: { code: "code", note: "note" },
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
  await screen.findByRole("option", { name: "observations" });
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

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Dataset importer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedBackend.list_datasets.mockResolvedValue(["observations"]);
    mockedBackend.stage_import.mockResolvedValue(stagedImport);
    mockedBackend.preview_import.mockResolvedValue(preview);
    mockedBackend.apply_import.mockResolvedValue({ success: true });
  });

  it("requires an explicit goal selection", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());

    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
    expect(screen.getAllByRole("radio")).toHaveLength(4);
    expect(
      screen
        .getAllByRole<HTMLInputElement>("radio")
        .every((option) => !option.checked),
    ).toBe(true);
    fireEvent.click(screen.getByRole("radio", { name: /Create/ }));
    expect(screen.getByRole("button", { name: "Next" })).toBeEnabled();
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

  it.each(["Merge", "Sync"] as const)(
    "disables missing target fields and supports composite identity for %s",
    async (goal) => {
      mockedBackend.stage_import.mockResolvedValue({
        ...stagedImport,
        fields: ["code", "note", "Bird Name"],
        source_mapping: {
          ...stagedImport.source_mapping,
          "Bird Name": "Bird_Name",
        },
      });
      render(<App />);
      await chooseExistingGoal(goal);
      const first = await screen.findByLabelText("Identity field 1");
      expect(within(first).getByRole("option", { name: "code" })).toBeEnabled();
      expect(
        within(first).getByRole("option", {
          name: "Bird Name → Bird_Name — Not in target dataset",
        }),
      ).toBeDisabled();
      fireEvent.change(first, { target: { value: "code" } });
      const second = screen.getByLabelText("Identity field 2");
      expect(
        within(second).queryByRole("option", { name: "code" }),
      ).not.toBeInTheDocument();
      fireEvent.change(second, { target: { value: "note" } });
      fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
      await screen.findByRole("button", { name: "Confirm import" });
      expect(mockedBackend.preview_import).toHaveBeenCalledWith({
        import_id: "import-1",
        identity_fields: ["code", "note"],
        update_policy: "imported",
      });
    },
  );

  it.each(["Merge", "Sync"] as const)(
    "blocks %s when no uploaded fields exist in the target",
    async (goal) => {
      mockedBackend.stage_import.mockResolvedValue({
        ...stagedImport,
        eligible_identity_fields: [],
      });
      render(<App />);
      await chooseExistingGoal(goal);
      expect(await screen.findByRole("status")).toHaveTextContent(
        "None of the uploaded fields map to columns in the target dataset",
      );
      for (const number of [1, 2, 3]) {
        expect(
          screen.getByLabelText(`Identity field ${number}`),
        ).toBeDisabled();
      }
      expect(
        screen.getByRole("button", { name: "Preview changes" }),
      ).toBeDisabled();
      expect(mockedBackend.preview_import).not.toHaveBeenCalled();
      fireEvent.click(screen.getByRole("button", { name: "Back" }));
      expect(
        screen.getByRole("button", { name: "Stage upload" }),
      ).toBeEnabled();
    },
  );

  it.each(["Merge", "Sync"] as const)(
    "preserves preview validation for an empty %s upload",
    async (goal) => {
      mockedBackend.stage_import.mockResolvedValue({
        ...stagedImport,
        fields: [],
        source_mapping: {},
        eligible_identity_fields: [],
        record_count: 0,
      });
      if (goal === "Sync")
        mockedBackend.preview_import.mockResolvedValue({
          validation_error: "Create and Sync imports cannot be empty.",
        });
      render(<App />);
      await chooseExistingGoal(goal);
      await screen.findByLabelText("Identity field 1");
      expect(
        screen.queryByText(/None of the uploaded fields map/),
      ).not.toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
      if (goal === "Sync") {
        expect(await screen.findByRole("alert")).toHaveTextContent(
          "cannot be empty",
        );
      } else {
        await screen.findByRole("button", { name: "Confirm import" });
      }
      expect(mockedBackend.preview_import).toHaveBeenCalledWith({
        import_id: "import-1",
        identity_fields: [],
        update_policy: "imported",
      });
    },
  );

  it("rejects an oversized file before staging", async () => {
    mockedBackend.check_dataset_name.mockResolvedValue({
      available: true,
      table_name: "birds",
    });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("radio", { name: /Create/ }));
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

  it("preserves prior input when navigating back", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("radio", { name: /Merge/ }));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.change(screen.getByLabelText("Dataset"), {
      target: { value: "observations" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Next" }));

    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.getByLabelText("Dataset")).toHaveValue("observations");
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.getByRole("radio", { name: /Merge/ })).toBeChecked();
  });

  it("shows an unavailable dataset name and blocks progress", async () => {
    mockedBackend.check_dataset_name.mockResolvedValue({
      available: false,
      table_name: "birds",
    });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("radio", { name: /Create/ }));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.change(screen.getByLabelText("Dataset name"), {
      target: { value: "Birds" },
    });

    expect(screen.getByRole("status")).toHaveTextContent(
      "Checking availability",
    );
    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent("already exists"),
    );
    expect(screen.getByPlaceholderText("Bird observations")).toBeInvalid();
    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
  });

  it("rejects an unsupported dropped file before staging", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("radio", { name: /Append/ }));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.change(screen.getByLabelText("Dataset"), {
      target: { value: "observations" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Next" }));

    const input = screen.getByLabelText(/Drop a file here/);
    fireEvent.drop(input.parentElement!, {
      dataTransfer: { files: [new File(["bad"], "rows.exe")] },
    });
    expect(screen.getByRole("alert")).toHaveTextContent("supported");
    expect(mockedBackend.stage_import).not.toHaveBeenCalled();
  });

  it("lists every supported upload format", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("radio", { name: /Append/ }));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.change(screen.getByLabelText("Dataset"), {
      target: { value: "observations" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Next" }));

    expect(screen.getByText(/Supported formats:/)).toHaveTextContent(
      "Supported formats: csv, geojson, gpx, gpkg, json, kml, zip, xls, xlsx, xml. Maximum source size: 25 MiB. GeoPackages must contain one spatial layer.",
    );
  });

  it("shows validation messages returned by Windmill", async () => {
    mockedBackend.stage_import.mockResolvedValue({
      validation_error:
        "Invalid file. CSV rows must match the header column count.",
    });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Append");

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Invalid file. CSV rows must match the header column count.",
    );
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

  it.each([
    "Incomplete uploaded identity in selected field 'code' at staged record ordinal(s): 2. Every selected identity field must be present and cannot be null, empty, or whitespace-only.",
    "Multiple existing records in the target dataset match an uploaded identity. Matching target identity combinations must be unambiguous under either update policy.",
  ])("shows preview identity validation: %s", async (validation_error) => {
    mockedBackend.preview_import.mockResolvedValue({ validation_error });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Merge");
    fireEvent.change(screen.getByLabelText("Identity field 1"), {
      target: { value: "code" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      validation_error,
    );
    expect(screen.getByLabelText("Identity field 1")).toHaveValue("code");
    expect(mockedBackend.apply_import).not.toHaveBeenCalled();
  });

  it.each([
    {
      body: {
        error: {
          message:
            'Traceback (most recent call last):\n  File "importer.py", line 1\nImportValidationError: Duplicate record identities were found in the uploaded file.',
        },
      },
    },
    {
      body: JSON.stringify({
        error: {
          message:
            "Duplicate record identities were found in the uploaded file.",
        },
      }),
    },
    {
      body: {
        detail: "Duplicate record identities were found in the uploaded file.",
      },
    },
  ])(
    "shows the preview failure reason from a rejected request (%j)",
    async (reason) => {
      const consoleError = vi
        .spyOn(console, "error")
        .mockImplementationOnce(() => {});
      mockedBackend.preview_import.mockRejectedValueOnce(reason);
      render(<App />);
      await waitFor(() =>
        expect(mockedBackend.list_datasets).toHaveBeenCalled(),
      );
      await chooseExistingGoal("Merge");
      fireEvent.change(screen.getByLabelText("Identity field 1"), {
        target: { value: "code" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));

      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent(
        "Duplicate record identities were found in the uploaded file.",
      );
      expect(alert).not.toHaveTextContent("Traceback");
      expect(alert).not.toHaveTextContent("importer.py");
      expect(screen.getByLabelText("Identity field 1")).toHaveValue("code");
      expect(consoleError).toHaveBeenCalledExactlyOnceWith(
        "Failed to preview import",
        reason,
      );
    },
  );

  it("reports imported map geometry coverage", async () => {
    mockedBackend.preview_import.mockResolvedValue({
      ...preview,
      geometry_invalid: 2,
      geometry_valid: 18,
    });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Append");
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));

    expect(await screen.findByRole("status")).toHaveTextContent(
      "18 of 20 imported records include valid map geometry",
    );
    expect(screen.getByRole("status")).toHaveTextContent(
      "2 imported records have no valid map geometry",
    );
  });

  it("shows incomplete coordinate-pair warnings", async () => {
    mockedBackend.stage_import.mockResolvedValue({
      ...stagedImport,
      geometry_warning:
        "Coordinate columns were incomplete, so no map geometry was generated.",
    });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Append");

    expect(screen.getByRole("status")).toHaveTextContent(
      "no map geometry was generated",
    );
  });

  it("requires identity and confirms destructive Sync explicitly", async () => {
    const destructivePreview = {
      ...preview,
      added: 0,
      deleted: 13,
      unchanged: 3,
      final_count: 3,
    };
    mockedBackend.stage_import.mockResolvedValue({
      ...stagedImport,
      record_count: 3,
    });
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
    await screen.findByText(/permanently delete 13 unmatched records/);
    expect(screen.getByText("Rows deleted").previousSibling).toHaveTextContent(
      "13",
    );
    expect(
      screen.getByText("Final record count").previousSibling,
    ).toHaveTextContent("3");
    fireEvent.click(screen.getByRole("button", { name: "Confirm import" }));

    expect(confirmation).toHaveBeenCalledWith(
      "Sync will delete 13 records from observations. Continue?",
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
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementationOnce(() => {});
    const reason = new Error("The target changed. Review the import again.");
    mockedBackend.apply_import.mockRejectedValue(reason);
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
    expect(consoleError).toHaveBeenCalledExactlyOnceWith(
      "Failed to apply import",
      reason,
    );
  });

  it("displays actual mappings and submits original source names", async () => {
    mockedBackend.stage_import.mockResolvedValue({
      ...stagedImport,
      fields: ["_id", "source_id", "Bird Name"],
      eligible_identity_fields: ["_id", "source_id", "Bird Name"],
      source_mapping: {
        _id: "source_id_002",
        source_id: "source_id",
        "Bird Name": "Bird_Name",
      },
    });
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Merge");
    expect(
      screen.getAllByRole("option", { name: "_id → source_id_002" })[0],
    ).toHaveValue("_id");
    expect(screen.getAllByRole("option", { name: "source_id" })[0]).toHaveValue(
      "source_id",
    );
    expect(
      screen.getAllByRole("option", { name: "Bird Name → Bird_Name" })[0],
    ).toHaveValue("Bird Name");
    expect(
      screen.getByText(
        /Every selected column in the updated file must contain complete values/,
      ),
    ).toBeVisible();
    expect(
      screen.getByText(/If multiple records share the same identity/),
    ).toBeVisible();
    fireEvent.change(screen.getByLabelText("Identity field 1"), {
      target: { value: "_id" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByRole("button", { name: "Confirm import" });
    expect(mockedBackend.preview_import).toHaveBeenCalledWith({
      identity_fields: ["_id"],
      import_id: "import-1",
      update_policy: "imported",
    });
    const toggle = screen.getByText("Field mappings");
    const details = toggle.closest("details")!;
    expect(details).not.toHaveAttribute("open");
    expect(
      screen.getByRole("table", { name: "Field mappings" }),
    ).not.toBeVisible();
    fireEvent.click(toggle);
    expect(details).toHaveAttribute("open");
    const table = screen.getByRole("table", { name: "Field mappings" });
    expect(
      within(table).getByRole("columnheader", { name: "Uploaded field" }),
    ).toBeVisible();
    expect(
      within(table).getByRole("columnheader", { name: "Stored column" }),
    ).toBeVisible();
    expect(
      within(table).getByRole("row", { name: "_id source_id_002" }),
    ).toBeVisible();
    expect(
      within(table).getByRole("row", { name: "source_id source_id" }),
    ).toBeVisible();
    expect(
      within(table).getByRole("row", { name: "Bird Name Bird_Name" }),
    ).toBeVisible();
    fireEvent.click(toggle);
    expect(details).not.toHaveAttribute("open");
    expect(
      screen.getByText("_id → source_id_002", { selector: "dd" }),
    ).toBeVisible();
  });

  it("shows mappings only after generating the review", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Append");
    await screen.findByRole("button", { name: "Preview changes" });
    expect(screen.queryByText("Field mappings")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("table", { hidden: true }),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByRole("button", { name: "Confirm import" });
    const toggle = screen.getByText("Field mappings");
    expect(toggle.closest("details")).not.toHaveAttribute("open");
    fireEvent.click(toggle);
    expect(screen.getByRole("table", { name: "Field mappings" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.queryByText("Field mappings")).not.toBeInTheDocument();
  });

  it("explains Sync deletions and empty ordinary field policy", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Sync");
    expect(
      screen.getByText(
        /Sync deletes every unmatched row in the entire selected table/,
      ),
    ).toHaveTextContent("records from other sources");
    expect(
      screen.getByText(/Existing records win does not prevent these deletions/),
    ).toBeVisible();
    expect(
      screen.getByText(/including empty ordinary field values/),
    ).toBeVisible();
  });

  it("allows re-review after an old-rule preview is rejected", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    mockedBackend.apply_import.mockRejectedValueOnce(
      new Error(
        "Identity rules changed since this preview. Review the import again.",
      ),
    );
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Merge");
    fireEvent.change(screen.getByLabelText("Identity field 1"), {
      target: { value: "code" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "Confirm import" }),
    );
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Identity rules changed",
    );
    expect(
      screen.queryByRole("button", { name: "Confirm import" }),
    ).not.toBeInTheDocument();
    mockedBackend.preview_import.mockResolvedValueOnce({
      ...preview,
      preview_id: "preview-current",
    });
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "Confirm import" }),
    );
    await screen.findByText("Import applied successfully.");
    expect(mockedBackend.preview_import).toHaveBeenLastCalledWith({
      identity_fields: ["code"],
      import_id: "import-1",
      update_policy: "imported",
    });
    expect(mockedBackend.apply_import).toHaveBeenLastCalledWith({
      import_id: "import-1",
      preview_id: "preview-current",
    });
    expect(mockedBackend.stage_import).toHaveBeenCalledTimes(1);
  });

  it("clears the journey after a successful import", async () => {
    render(<App />);
    await waitFor(() => expect(mockedBackend.list_datasets).toHaveBeenCalled());
    await chooseExistingGoal("Append");
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByRole("button", { name: "Confirm import" });
    fireEvent.click(screen.getByRole("button", { name: "Confirm import" }));
    await screen.findByText("Import applied successfully.");
    fireEvent.click(
      screen.getByRole("button", { name: "Import another dataset" }),
    );

    expect(
      screen.getByRole("heading", { name: "What's your goal?" }),
    ).toBeVisible();
    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
    expect(
      screen
        .getAllByRole<HTMLInputElement>("radio")
        .every((option) => !option.checked),
    ).toBe(true);
  });
});
