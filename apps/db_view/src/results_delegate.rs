use std::collections::{HashMap, HashSet};

use db::{FieldType, TableColumnMeta};
use gpui::{div, px, App, AppContext, Context, Entity, IntoElement, InteractiveElement as _, ParentElement as _, Styled, Subscription, Window};
use gpui_component::input::{InputEvent, InputState};
use gpui_component::table::Column;
use gpui_component::{h_flex, table::{ TableDelegate, TableState}};
use gpui_component::table::filter_panel::FilterValue;

/// Represents a single cell change with old and new values
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellChange {
    pub col_ix: usize,
    pub col_name: String,
    pub old_value: String,
    pub new_value: String,
}

/// Represents the status of a row
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowStatus {
    /// Original data, unchanged
    Original,
    /// Newly added row
    New,
    /// Modified row
    Modified,
    /// Marked for deletion
    Deleted,
}

/// Represents a change to a row with detailed tracking
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowChange {
    /// A new row was added
    Added {
        /// Data for the new row
        data: Vec<String>,
    },
    /// An existing row was updated
    Updated {
        /// Original row data (for generating WHERE clause)
        original_data: Vec<String>,
        /// Changed cells only
        changes: Vec<CellChange>,
    },
    /// A row was marked for deletion
    Deleted {
        /// Original data (for generating WHERE clause)
        original_data: Vec<String>,
    },
}



pub struct EditorTableDelegate {
    pub columns: Vec<Column>,
    /// Column metadata with type information
    pub column_meta: Vec<TableColumnMeta>,
    pub rows: Vec<Vec<String>>,
    /// Original data snapshot for change detection
    original_rows: Vec<Vec<String>>,
    /// Track row status: key is current row index
    row_status: HashMap<usize, RowStatus>,
    /// Track modified cells (row_ix, col_ix) -> (old_value, new_value)
    cell_changes: HashMap<(usize, usize), (String, String)>,
    /// Track modified cells for UI highlighting
    pub modified_cells: HashSet<(usize, usize)>,
    /// Rows marked for deletion (original row indices)
    deleted_original_rows: HashSet<usize>,
    /// Mapping from current row index to original row index (for tracking)
    row_index_map: HashMap<usize, usize>,
    /// Next row index for new rows (negative conceptually, but we use high numbers)
    next_new_row_id: usize,
    /// New rows data: key is the new_row_id
    new_rows: HashMap<usize, Vec<String>>,
    /// Primary key column indices
    primary_key_columns: Vec<usize>,
    /// Active filter columns (for UI indication)
    active_filter_columns: HashSet<usize>,
    /// Filtered row indices (None means no filter, show all rows)
    /// When set, only these row indices from `rows` will be displayed
    filtered_row_indices: Option<Vec<usize>>,
    /// Column filter conditions: col_ix -> selected values
    column_filters: HashMap<usize, HashSet<String>>,
}

impl Clone for EditorTableDelegate {
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            column_meta: self.column_meta.clone(),
            rows: self.rows.clone(),
            original_rows: self.original_rows.clone(),
            row_status: self.row_status.clone(),
            cell_changes: self.cell_changes.clone(),
            modified_cells: self.modified_cells.clone(),
            deleted_original_rows: self.deleted_original_rows.clone(),
            row_index_map: self.row_index_map.clone(),
            next_new_row_id: self.next_new_row_id,
            new_rows: self.new_rows.clone(),
            primary_key_columns: self.primary_key_columns.clone(),
            active_filter_columns: self.active_filter_columns.clone(),
            filtered_row_indices: self.filtered_row_indices.clone(),
            column_filters: self.column_filters.clone(),
        }
    }
}

impl EditorTableDelegate {
    pub fn new(columns: Vec<Column>, rows: Vec<Vec<String>>, _window: &mut Window, _cx: &mut Context<TableState<Self>>) -> Self {
        let row_count = rows.len();
        let row_index_map: HashMap<usize, usize> = (0..row_count).map(|i| (i, i)).collect();
        Self {
            columns,
            column_meta: Vec::new(),
            original_rows: rows.clone(),
            rows,
            row_status: HashMap::new(),
            cell_changes: HashMap::new(),
            modified_cells: HashSet::new(),
            deleted_original_rows: HashSet::new(),
            row_index_map,
            next_new_row_id: 1_000_000,
            new_rows: HashMap::new(),
            primary_key_columns: Vec::new(),
            active_filter_columns: HashSet::new(),
            filtered_row_indices: None,
            column_filters: HashMap::new(),
        }
    }

    /// Set column metadata
    pub fn set_column_meta(&mut self, meta: Vec<TableColumnMeta>) {
        self.column_meta = meta;
    }

    /// Get column metadata
    pub fn column_meta(&self) -> &[TableColumnMeta] {
        &self.column_meta
    }

    /// Get field type for a column
    pub fn get_field_type(&self, col_ix: usize) -> FieldType {
        self.column_meta
            .get(col_ix)
            .map(|m| m.field_type)
            .unwrap_or(FieldType::Unknown)
    }

    /// Set primary key column indices
    pub fn set_primary_keys(&mut self, pk_columns: Vec<usize>) {
        self.primary_key_columns = pk_columns;
    }

    /// Get primary key column indices
    pub fn primary_key_columns(&self) -> &[usize] {
        &self.primary_key_columns
    }

    /// Record a cell change (used by external editors like large text editor)
    ///
    /// This method handles tracking cell changes and updating row status.
    /// It's similar to `on_cell_edited` but can be called directly from external code.
    pub fn record_cell_change(&mut self, row_ix: usize, col_ix: usize, new_value: String) -> bool {
        // Get old value from current rows
        let Some(row) = self.rows.get_mut(row_ix) else {
            return false;
        };
        let Some(cell) = row.get_mut(col_ix) else {
            return false;
        };

        // If value hasn't changed, don't record
        if *cell == new_value {
            self.modified_cells.retain(|&(r, c)| r != row_ix || c != col_ix);
            return false;
        }

        let old_value = cell.clone();
        *cell = new_value.clone();

        // Mark cell as modified for UI
        self.modified_cells.insert((row_ix, col_ix));

        // Track the change only if not a new row
        if !self.is_new_row(row_ix) {
            self.cell_changes
                .entry((row_ix, col_ix))
                .and_modify(|(_, new)| *new = new_value.clone())
                .or_insert((old_value, new_value));

            // Update row status
            self.row_status.insert(row_ix, RowStatus::Modified);
        }

        true
    }

    pub fn update_data(&mut self, columns: Vec<Column>, rows: Vec<Vec<String>>, _cx: &mut App) {
        // Calculate column widths based on content
        let mut col_widths: Vec<usize> = columns.iter().map(|c| c.name.len()).collect();
        
        for row in &rows {
            for (col_ix, cell) in row.iter().enumerate() {
                if col_ix < col_widths.len() {
                    col_widths[col_ix] = col_widths[col_ix].max(cell.len());
                }
            }
        }

        // Set column widths and make sortable (min 60px, max 300px, ~8px per char)
        self.columns = columns
            .into_iter()
            .enumerate()
            .map(|(ix, mut col)| {
                let char_width = col_widths.get(ix).copied().unwrap_or(10);
                // Add extra width for filter/sort icons
                let width = ((char_width * 8) + 60).max(80).min(300);
                col.width = px(width as f32);
                // Make column sortable
                col = col.sortable();
                col
            })
            .collect();

        let row_count = rows.len();
        self.original_rows = rows.clone();
        self.rows = rows.clone();
        self.row_index_map = (0..row_count).map(|i| (i, i)).collect();
        
        // Clear all change tracking
        self.clear_changes();
    }

    /// Get all pending changes for saving to database
    pub fn get_changes(&self) -> Vec<RowChange> {
        let mut changes = Vec::new();

        // Collect deleted rows
        for &original_ix in &self.deleted_original_rows {
            if let Some(original_data) = self.original_rows.get(original_ix) {
                changes.push(RowChange::Deleted {
                    original_data: original_data.clone(),
                });
            }
        }

        // Collect modified rows
        let mut modified_rows: HashMap<usize, Vec<CellChange>> = HashMap::new();
        for (&(row_ix, col_ix), (old_val, new_val)) in &self.cell_changes {
            // Skip if this row is deleted
            if let Some(&original_ix) = self.row_index_map.get(&row_ix) {
                if self.deleted_original_rows.contains(&original_ix) {
                    continue;
                }
            }

            let col_name = self
                .columns
                .get(col_ix)
                .map(|c| c.name.to_string())
                .unwrap_or_default();

            modified_rows
                .entry(row_ix)
                .or_default()
                .push(CellChange {
                    col_ix,
                    col_name,
                    old_value: old_val.clone(),
                    new_value: new_val.clone(),
                });
        }

        for (row_ix, cell_changes) in modified_rows {
            if let Some(&original_ix) = self.row_index_map.get(&row_ix) {
                if let Some(original_data) = self.original_rows.get(original_ix) {
                    changes.push(RowChange::Updated {
                        original_data: original_data.clone(),
                        changes: cell_changes,
                    });
                }
            }
        }

        // Collect new rows
        for (_, data) in &self.new_rows {
            changes.push(RowChange::Added { data: data.clone() });
        }

        changes
    }

    /// Clear all pending changes
    pub fn clear_changes(&mut self) {
        self.row_status.clear();
        self.cell_changes.clear();
        self.modified_cells.clear();
        self.deleted_original_rows.clear();
        self.new_rows.clear();
    }

    /// Revert all changes and restore to original state
    ///
    /// This method:
    /// 1. Restores row data to original values
    /// 2. Removes all newly added rows
    /// 3. Restores deleted rows
    /// 4. Clears all change tracking
    /// 5. Recalculates filter results
    pub fn revert_all_changes(&mut self) {
        // Restore rows to original state
        self.rows = self.original_rows.clone();

        // Restore row_index_map for restored rows
        let row_count = self.rows.len();
        self.row_index_map = (0..row_count).map(|i| (i, i)).collect();

        // Clear all change tracking
        self.clear_changes();

        // Recalculate filter results with restored data
        if !self.column_filters.is_empty() {
            self.recalculate_filtered_indices();
        }
    }

    /// Check if there are any pending changes
    pub fn has_changes(&self) -> bool {
        !self.cell_changes.is_empty()
            || !self.deleted_original_rows.is_empty()
            || !self.new_rows.is_empty()
    }

    /// Get the count of pending changes
    pub fn changes_count(&self) -> usize {
        let modified_rows: HashSet<usize> = self.cell_changes.keys().map(|(r, _)| *r).collect();
        modified_rows.len() + self.deleted_original_rows.len() + self.new_rows.len()
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.to_string()).collect()
    }

    /// Check if a row is newly added
    pub fn is_new_row(&self, row_ix: usize) -> bool {
        self.row_status.get(&row_ix) == Some(&RowStatus::New)
    }

    /// Check if a row is marked for deletion
    pub fn is_deleted_row(&self, row_ix: usize) -> bool {
        self.row_status.get(&row_ix) == Some(&RowStatus::Deleted)
    }

    /// Set active filter columns for UI indication
    pub fn set_active_filter_columns(&mut self, columns: HashSet<usize>) {
        self.active_filter_columns = columns;
    }

    /// Set filtered row indices for display
    /// 
    /// When set to Some(indices), only these rows will be displayed in the table.
    /// When set to None, all rows will be displayed.
    /// 
    /// Requirements: 5.1, 5.5
    pub fn set_filtered_indices(&mut self, indices: Option<Vec<usize>>) {
        self.filtered_row_indices = indices;
    }

    /// Get the actual row index from the display row index
    /// 
    /// When filtering is active, the display row index (0, 1, 2...) needs to be
    /// mapped to the actual row index in the full dataset.
    fn map_display_to_actual_row(&self, display_row_ix: usize) -> usize {
        if let Some(ref indices) = self.filtered_row_indices {
            indices.get(display_row_ix).copied().unwrap_or(display_row_ix)
        } else {
            display_row_ix
        }
    }

    /// Get the filtered row count (for display)
    pub fn filtered_row_count(&self) -> usize {
        if let Some(ref indices) = self.filtered_row_indices {
            indices.len()
        } else {
            self.rows.len()
        }
    }

    pub fn resolve_display_row(&self, display_row_ix: usize) -> Option<usize> {
        if let Some(ref indices) = self.filtered_row_indices {
            indices.get(display_row_ix).copied()
        } else if display_row_ix < self.rows.len() {
            Some(display_row_ix)
        } else {
            None
        }
    }



    // ============================================================================
    // Column Filter Methods (to be called from external code)
    // ============================================================================

    /// 应用筛选到数据（支持多列筛选）
    pub fn apply_filter(&mut self, col_ix: usize, selected_values: HashSet<String>) {
        // 存储该列的筛选条件
        self.column_filters.insert(col_ix, selected_values);
        self.active_filter_columns.insert(col_ix);
        
        // 重新计算所有筛选条件的组合结果
        self.recalculate_filtered_indices();
    }

    /// 清除单列筛选
    pub fn clear_column_filter(&mut self, col_ix: usize) {
        self.column_filters.remove(&col_ix);
        self.active_filter_columns.remove(&col_ix);
        
        // 重新计算筛选结果
        self.recalculate_filtered_indices();
    }

    /// 清除所有筛选
    pub fn clear_all_filters(&mut self) {
        self.column_filters.clear();
        self.active_filter_columns.clear();
        self.filtered_row_indices = None;
    }

    /// 重新计算筛选后的行索引（多列 AND 组合）
    fn recalculate_filtered_indices(&mut self) {
        if self.column_filters.is_empty() {
            self.filtered_row_indices = None;
            return;
        }

        let filtered_indices: Vec<usize> = self.rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                // 所有筛选条件都必须满足（AND）
                self.column_filters.iter().all(|(&col_ix, selected_values)| {
                    let cell_value = row.get(col_ix).map(|s| s.as_str()).unwrap_or("NULL");
                    selected_values.contains(cell_value)
                })
            })
            .map(|(ix, _)| ix)
            .collect();

        // 如果筛选后的行数等于总行数，说明没有实际筛选效果
        if filtered_indices.len() == self.rows.len() {
            self.filtered_row_indices = None;
        } else {
            self.filtered_row_indices = Some(filtered_indices);
        }
    }

}

impl TableDelegate for EditorTableDelegate {
    fn row_number_enabled(&self, _cx: &App) -> bool {
        true
    }
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        // Return filtered row count if filtering is active
        self.filtered_row_count()
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns[col_ix].clone()
    }

    fn render_th(&mut self, col_ix: usize, _window: &mut Window, _: &mut Context<TableState<Self>>) -> impl IntoElement {
        let col_name = self
            .columns
            .get(col_ix)
            .map(|c| c.name.clone())
            .unwrap_or_default();

        h_flex()
            .size_full()
            .items_center()
            .justify_between()
            .gap_1()
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(col_name),
            )
    }

    fn render_td(
        &mut self,
        row: usize,
        col: usize,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        // Map display row index to actual row index
        let actual_row = self.map_display_to_actual_row(row);

        self.rows
            .get(actual_row)
            .and_then(|r| r.get(col))
            .cloned()
            .unwrap_or_default()
    }

    fn build_input(&self, row_ix: usize, col_ix: usize, window: &mut Window, cx: &mut Context<TableState<Self>>) -> Option<(Entity<InputState>, Subscription)> {
        // Map display row index to actual row index
        let actual_row = self.map_display_to_actual_row(row_ix);

        if self.is_deleted_row(actual_row) {
            return None;
        }

        let value = self.rows
            .get(actual_row)
            .and_then(|r| r.get(col_ix))
            .cloned()
            .unwrap_or_default();
        // Create input state with the current value (support multiline)
        let input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).multi_line(true).rows(1).auto_grow(1,1);
            state.set_value(value, window, cx);
            state.focus(window, cx);
            state
        });

        let _sub = cx.subscribe_in(&input, window,move |table, _, evt: &InputEvent, window, cx| {
            match evt {
                InputEvent::Blur => {
                    table.commit_cell_edit(window,cx);
                }
                _ => {}
            }
        });
        Some((input, _sub))
    }

    fn on_cell_edited(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        new_value: String,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) -> bool {
        // Map display row index to actual row index
        let actual_row = self.map_display_to_actual_row(row_ix);

        // Update the cell value
        if let Some(row) = self.rows.get_mut(actual_row) {
            if let Some(cell) = row.get_mut(col_ix) {
                // Only mark as modified if value actually changed
                if *cell == new_value {
                    self.modified_cells.retain(|&(row_ix, col_ix)| row_ix != actual_row || col_ix != col_ix);
                    return false;
                }

                let old_value = cell.clone();
                *cell = new_value.clone();

                // Mark cell as modified for UI (use actual row index)
                self.modified_cells.insert((actual_row, col_ix));

                // Track the change with old and new values
                // If this is a new row, we don't need to track cell changes
                if self.is_new_row(actual_row) {
                    // Just update the new_rows data
                    if let Some(new_row_id) = self.find_new_row_id(actual_row) {
                        if let Some(new_row_data) = self.new_rows.get_mut(&new_row_id) {
                            if let Some(cell) = new_row_data.get_mut(col_ix) {
                                *cell = new_value;
                            }
                        }
                    }
                } else {
                    // For existing rows, track the cell change
                    // If we already have a change for this cell, keep the original old_value
                    self.cell_changes
                        .entry((actual_row, col_ix))
                        .and_modify(|(_, new)| *new = new_value.clone())
                        .or_insert((old_value, new_value));

                    // Update row status
                    self.row_status.insert(actual_row, RowStatus::Modified);
                }

                return true;
            }
        }
        false
    }

    fn is_cell_modified(&self, row_ix: usize, col_ix: usize, _cx: &App) -> bool {
        // Map display row index to actual row index
        let actual_row = self.map_display_to_actual_row(row_ix);
        self.modified_cells.contains(&(actual_row, col_ix))
    }

    fn is_row_deleted(&self, row_ix: usize, _cx: &App) -> bool {
        // Map display row index to actual row index
        let actual_row = self.map_display_to_actual_row(row_ix);
        self.is_deleted_row(actual_row)
    }

    fn on_row_added(&mut self, _window: &mut Window, _cx: &mut Context<TableState<Self>>) -> usize {
        // Add a new empty row
        let new_row = vec!["".to_string(); self.columns.len()];
        let row_ix = self.rows.len();
        self.rows.push(new_row.clone());

        // Track as new row
        let new_row_id = self.next_new_row_id;
        self.next_new_row_id += 1;
        self.new_rows.insert(new_row_id, new_row);
        self.row_status.insert(row_ix, RowStatus::New);

        // Map the new row index to the new_row_id (using high number as marker)
        self.row_index_map.insert(row_ix, new_row_id);

        self.next_new_row_id
    }

    fn on_row_deleted(
        &mut self,
        row_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        if row_ix >= self.rows.len() {
            return;
        }

        // Check if this is a new row (not yet saved to DB)
        if self.is_new_row(row_ix) {
            // For new rows, remove them immediately since they don't exist in DB
            if let Some(new_row_id) = self.find_new_row_id(row_ix) {
                self.new_rows.remove(&new_row_id);
            }
            self.rows.remove(row_ix);
            self.row_status.remove(&row_ix);
            self.row_index_map.remove(&row_ix);

            // Re-index rows after deletion
            self.reindex_after_deletion(row_ix);
        } else {
            // For existing rows (from DB), only mark as deleted but keep the row visible
            // This allows users to see deleted rows with special styling and undo the deletion
            if let Some(&original_ix) = self.row_index_map.get(&row_ix) {
                self.deleted_original_rows.insert(original_ix);
            }
            self.row_status.insert(row_ix, RowStatus::Deleted);

            // Keep the row in display - don't remove it yet
            // It will be visually marked as deleted (e.g., strikethrough)
            // and will only be truly removed when changes are submitted
        }

        // Clean up cell changes for deleted row
        self.cell_changes.retain(|&(r, _), _| r != row_ix);
        self.modified_cells.retain(|&(r, _)| r != row_ix);

        cx.notify();
    }

    fn column_filter_enabled(&self, _cx: &App) -> bool {
        true
    }

    fn get_column_filter_values(&self, col_ix: usize, _cx: &App) -> Vec<FilterValue> {
        use std::collections::HashMap;

        let mut value_counts: HashMap<String, usize> = HashMap::new();

        // 获取其他列的筛选条件（排除当前列）
        let other_filters: HashMap<usize, &HashSet<String>> = self.column_filters
            .iter()
            .filter(|(c, _)| **c != col_ix)
            .map(|(c, v)| (*c, v))
            .collect();

        for (_row_ix, row) in self.rows.iter().enumerate() {
            // 检查该行是否满足其他列的筛选条件
            let passes_other_filters = other_filters.iter().all(|(&other_col, selected_values)| {
                let cell_value = row.get(other_col).map(|s| s.as_str()).unwrap_or("NULL");
                selected_values.contains(cell_value)
            });

            if passes_other_filters {
                let value = row
                    .get(col_ix)
                    .cloned()
                    .unwrap_or_else(|| "NULL".to_string());
                *value_counts.entry(value).or_insert(0) += 1;
            }
        }

        let mut result: Vec<_> = value_counts
            .into_iter()
            .map(|(value, count)| FilterValue::new(value, count))
            .collect();
        result.sort_by(|a, b| a.value.cmp(&b.value));
        result
    }

    fn is_column_filtered(&self, col_ix: usize, _cx: &App) -> bool {
        self.active_filter_columns.contains(&col_ix)
    }

    fn on_column_filter_changed(
        &mut self,
        col_ix: usize,
        selected_values: HashSet<String>,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) {
        self.apply_filter(col_ix, selected_values);
    }

    fn on_column_filter_cleared(
        &mut self,
        col_ix: usize,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) {
        self.clear_column_filter(col_ix);
    }
}

impl EditorTableDelegate {
    /// Find the new_row_id for a given row index
    fn find_new_row_id(&self, row_ix: usize) -> Option<usize> {
        self.row_index_map.get(&row_ix).copied().filter(|&id| id >= 1_000_000)
    }

    /// Re-index rows after a deletion
    fn reindex_after_deletion(&mut self, deleted_ix: usize) {
        // Update row_index_map: shift all indices after deleted_ix
        let mut new_map = HashMap::new();
        for (&row_ix, &original_ix) in &self.row_index_map {
            if row_ix > deleted_ix {
                new_map.insert(row_ix - 1, original_ix);
            } else if row_ix < deleted_ix {
                new_map.insert(row_ix, original_ix);
            }
            // Skip the deleted row
        }
        self.row_index_map = new_map;

        // Update row_status
        let mut new_status = HashMap::new();
        for (&row_ix, &status) in &self.row_status {
            if row_ix > deleted_ix {
                new_status.insert(row_ix - 1, status);
            } else if row_ix < deleted_ix {
                new_status.insert(row_ix, status);
            }
        }
        self.row_status = new_status;

        // Update cell_changes
        let mut new_changes = HashMap::new();
        for (&(row_ix, col_ix), change) in &self.cell_changes {
            if row_ix > deleted_ix {
                new_changes.insert((row_ix - 1, col_ix), change.clone());
            } else if row_ix < deleted_ix {
                new_changes.insert((row_ix, col_ix), change.clone());
            }
        }
        self.cell_changes = new_changes;

        // Update modified_cells
        let mut new_modified = HashSet::new();
        for &(row_ix, col_ix) in &self.modified_cells {
            if row_ix > deleted_ix {
                new_modified.insert((row_ix - 1, col_ix));
            } else if row_ix < deleted_ix {
                new_modified.insert((row_ix, col_ix));
            }
        }
        self.modified_cells = new_modified;
    }
}


