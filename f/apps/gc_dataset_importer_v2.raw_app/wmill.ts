// THIS FILE IS READ-ONLY
// AND GENERATED AUTOMATICALLY FROM YOUR RUNNABLES

type Goal = "create" | "append" | "merge" | "sync";
type Policy = "imported" | "existing";

type StagedImport = {
  fields: string[];
  geometry_warning?: string;
  import_id: string;
  record_count: number;
  source_format: string;
};

type ValidationFailure = { validation_error: string };

type Preview = {
  added: number;
  columns_added: number;
  deleted: number;
  final_count: number;
  geometry_invalid?: number;
  geometry_valid?: number;
  unchanged: number;
  updated: number;
  preview_id: string;
};

export declare const backend: {
  list_datasets: () => Promise<string[]>;
  check_dataset_name: (args: {
    dataset_name: string;
  }) => Promise<{ available: boolean; table_name: string }>;
  stage_import: (args: {
    goal: Goal;
    target_table: string;
    uploaded_file: { data: string; name: string };
  }) => Promise<StagedImport | ValidationFailure>;
  preview_import: (args: {
    import_id: string;
    identity_fields: string[];
    update_policy: Policy;
  }) => Promise<Preview | ValidationFailure>;
  apply_import: (args: {
    import_id: string;
    preview_id: string;
  }) => Promise<{ success: boolean }>;
};
