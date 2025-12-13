// 1. 标准库导入
use std::{collections::HashSet, ops::Range, rc::Rc, time::Duration};

// 2. 外部 crate 导入
use gpui::{canvas, div, prelude::FluentBuilder, px, uniform_list, AppContext, Axis, Bounds, ClickEvent, Context, Div, DragMoveEvent, ElementId, Entity, EventEmitter, FocusHandle, Focusable, InteractiveElement, IntoElement, ListSizingBehavior, MouseButton, MouseDownEvent, ParentElement, Pixels, Point, Render, ScrollStrategy, SharedString, Stateful, StatefulInteractiveElement as _, Styled, Subscription, Task, UniformListScrollHandle, Window};

// 3. 当前 crate 导入
use crate::{
    actions::{Cancel, Confirm, SelectDown, SelectUp},
    h_flex,
    input::{Input, InputState},
    menu::{ContextMenuExt, PopupMenu},
    scroll::{ScrollableMask, Scrollbar},
    v_flex, ActiveTheme, Icon, IconName, StyleSized as _, StyledExt,
    VirtualListScrollHandle,
};
use crate::list::{List, ListState};
use crate::table::filter_panel::FilterPanel;
use super::*;
use super::filter_state::FilterState;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SelectionState {
    Column,
    Row,
    Cell
}

/// The Table event.
#[derive(Clone)]
pub enum TableEvent {
    /// Single click or move to selected row.
    SelectRow(usize),
    /// Double click on the row.
    DoubleClickedRow(usize),
    /// Selected column.
    SelectColumn(usize),
    /// Selected cell (row_ix, col_ix).
    SelectCell(usize, usize),
    /// The column widths have changed.
    ///
    /// The `Vec<Pixels>` contains the new widths of all columns.
    ColumnWidthsChanged(Vec<Pixels>),
    /// A column has been moved.
    ///
    /// The first `usize` is the original index of the column,
    /// and the second `usize` is the new index of the column.
    MoveColumn(usize, usize),
    /// A cell is being edited.
    CellEditing(usize, usize),
    /// A cell edit was committed.
    CellEdited(usize, usize),
    /// A row was added.
    RowAdded,
    /// A row was deleted.
    RowDeleted(usize),
}

/// The visible range of the rows and columns.
#[derive(Debug, Default)]
pub struct TableVisibleRange {
    /// The visible range of the rows.
    rows: Range<usize>,
    /// The visible range of the columns.
    cols: Range<usize>,
}

impl TableVisibleRange {
    /// Returns the visible range of the rows.
    pub fn rows(&self) -> &Range<usize> {
        &self.rows
    }

    /// Returns the visible range of the columns.
    pub fn cols(&self) -> &Range<usize> {
        &self.cols
    }
}

/// The state for [`Table`].
pub struct TableState<D: TableDelegate> {
    focus_handle: FocusHandle,
    delegate: D,
    pub(super) options: TableOptions,
    /// The bounds of the table container.
    bounds: Bounds<Pixels>,
    /// The bounds of the fixed head cols.
    fixed_head_cols_bounds: Bounds<Pixels>,

    col_groups: Vec<ColGroup>,

    /// Whether the table can loop selection, default is true.
    ///
    /// When the prev/next selection is out of the table bounds, the selection will loop to the other side.
    pub loop_selection: bool,
    /// Whether the table can select column.
    pub col_selectable: bool,
    /// Whether the table can select row.
    pub row_selectable: bool,
    /// Whether the table can sort.
    pub sortable: bool,
    /// Whether the table can resize columns.
    pub col_resizable: bool,
    /// Whether the table can move columns.
    pub col_movable: bool,
    /// Enable/disable fixed columns feature.
    pub col_fixed: bool,
    /// Enable/disable column filtering feature.
    pub col_filterable: bool,

    pub vertical_scroll_handle: UniformListScrollHandle,
    pub horizontal_scroll_handle: VirtualListScrollHandle,

    selected_row: Option<usize>,
    selection_state: SelectionState,
    right_clicked_row: Option<usize>,
    selected_col: Option<usize>,
    /// The cell that is currently selected (row_ix, col_ix).
    selected_cell: Option<(usize, usize)>,
    /// The column index that is being resized.
    resizing_col: Option<usize>,

    /// The cell that is currently being edited (row_ix, col_ix).
    editing_cell: Option<(usize, usize)>,

    /// The input state for the cell being edited.
    editing_input: Option<Entity<InputState>>,

    _sub: Option<Subscription>,

    /// The visible range of the rows and columns.
    visible_range: TableVisibleRange,

    /// Filter state for column filtering.
    filter_state: FilterState,

    filter_list: Option<Entity<ListState<FilterPanel>>>,

    /// 当前打开的筛选面板的列索引（用于跟踪哪个筛选面板是打开的）
    active_filter_col: Option<usize>,

    _measure: Vec<Duration>,
    _load_more_task: Task<()>,
}

impl<D> TableState<D>
where
    D: TableDelegate,
{
    /// Create a new TableState with the given delegate.
    pub fn new(delegate: D, _: &mut Window, cx: &mut Context<Self>) -> Self {
        let mut this = Self {
            focus_handle: cx.focus_handle(),
            options: TableOptions::default(),
            delegate,
            col_groups: Vec::new(),
            horizontal_scroll_handle: VirtualListScrollHandle::new(),
            vertical_scroll_handle: UniformListScrollHandle::new(),
            selection_state: SelectionState::Row,
            selected_row: None,
            right_clicked_row: None,
            selected_col: None,
            selected_cell: None,
            resizing_col: None,
            editing_cell: None,
            editing_input: None,
            bounds: Bounds::default(),
            fixed_head_cols_bounds: Bounds::default(),
            visible_range: TableVisibleRange::default(),
            filter_state: FilterState::new(),
            filter_list: None,
            active_filter_col: None,
            loop_selection: true,
            col_selectable: true,
            row_selectable: true,
            sortable: true,
            col_movable: true,
            col_resizable: true,
            col_fixed: true,
            col_filterable: true,
            _load_more_task: Task::ready(()),
            _measure: Vec::new(),
            _sub: None,
        };

        this.prepare_col_groups(cx);
        this
    }

    /// Returns a reference to the delegate.
    pub fn delegate(&self) -> &D {
        &self.delegate
    }

    /// Returns a mutable reference to the delegate.
    pub fn delegate_mut(&mut self) -> &mut D {
        &mut self.delegate
    }

    /// Set to loop selection, default to true.
    pub fn loop_selection(mut self, loop_selection: bool) -> Self {
        self.loop_selection = loop_selection;
        self
    }

    /// Set to enable/disable column movable, default to true.
    pub fn col_movable(mut self, col_movable: bool) -> Self {
        self.col_movable = col_movable;
        self
    }

    /// Set to enable/disable column resizable, default to true.
    pub fn col_resizable(mut self, col_resizable: bool) -> Self {
        self.col_resizable = col_resizable;
        self
    }

    /// Set to enable/disable column sortable, default true
    pub fn sortable(mut self, sortable: bool) -> Self {
        self.sortable = sortable;
        self
    }

    /// Set to enable/disable row selectable, default true
    pub fn row_selectable(mut self, row_selectable: bool) -> Self {
        self.row_selectable = row_selectable;
        self
    }

    /// Set to enable/disable column selectable, default true
    pub fn col_selectable(mut self, col_selectable: bool) -> Self {
        self.col_selectable = col_selectable;
        self
    }

    /// When we update columns or rows, we need to refresh the table.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.prepare_col_groups(cx);
    }

    /// Scroll to the row at the given index.
    pub fn scroll_to_row(&mut self, row_ix: usize, cx: &mut Context<Self>) {
        self.vertical_scroll_handle
            .scroll_to_item(row_ix, ScrollStrategy::Top);
        cx.notify();
    }

    // Scroll to the column at the given index.
    pub fn scroll_to_col(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        let col_ix = col_ix.saturating_sub(self.fixed_left_cols_count());

        self.horizontal_scroll_handle
            .scroll_to_item(col_ix, ScrollStrategy::Top);
        cx.notify();
    }

    /// Returns the selected row index.
    pub fn selected_row(&self) -> Option<usize> {
        self.selected_row
    }

    /// Sets the selected row to the given index.
    pub fn set_selected_row(&mut self, row_ix: usize, cx: &mut Context<Self>) {
        let is_down = match self.selected_row {
            Some(selected_row) => row_ix > selected_row,
            None => true,
        };

        self.selection_state = SelectionState::Row;
        self.right_clicked_row = None;
        self.selected_row = Some(row_ix);
        self.selected_col = None;
        self.selected_cell = None;
        if let Some(row_ix) = self.selected_row {
            self.vertical_scroll_handle.scroll_to_item(
                row_ix,
                if is_down {
                    ScrollStrategy::Bottom
                } else {
                    ScrollStrategy::Top
                },
            );
        }
        cx.emit(TableEvent::SelectRow(row_ix));
        cx.notify();
    }

    /// Returns the selected column index.
    pub fn selected_col(&self) -> Option<usize> {
        self.selected_col
    }

    /// Sets the selected col to the given index.
    pub fn set_selected_col(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        self.selection_state = SelectionState::Column;
        self.selected_col = Some(col_ix);
        self.selected_row = None;
        self.selected_cell = None;
        if let Some(col_ix) = self.selected_col {
            self.scroll_to_col(col_ix, cx);
        }
        cx.emit(TableEvent::SelectColumn(col_ix));
        cx.notify();
    }

    /// Clear the selection of the table.
    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        self.selection_state = SelectionState::Row;
        self.selected_row = None;
        self.selected_col = None;
        self.selected_cell = None;
        cx.notify();
    }


    pub fn selected_cell(&self) -> Option<(usize, usize)> {
        self.selected_cell
    }

    pub fn set_selected_cell(&mut self, row_ix: usize, col_ix: usize, cx: &mut Context<Self>) {
        self.selection_state = SelectionState::Cell;
        self.selected_cell = Some((row_ix, col_ix));
        self.selected_col = None;
        self.selected_row = None;
        if let Some(col_ix) = self.selected_col {
            self.scroll_to_col(col_ix, cx);
        }
        cx.emit(TableEvent::SelectCell(row_ix, col_ix));
        cx.notify();
    }

    /// Returns the visible range of the rows and columns.
    ///
    /// See [`TableVisibleRange`].
    pub fn visible_range(&self) -> &TableVisibleRange {
        &self.visible_range
    }

    /// Returns a reference to the filter state.
    pub fn filter_state(&self) -> &FilterState {
        &self.filter_state
    }

    /// Returns a mutable reference to the filter state.
    pub fn filter_state_mut(&mut self) -> &mut FilterState {
        &mut self.filter_state
    }

    /// Set filter for a column.
    pub fn set_column_filter(&mut self, col_ix: usize, selected_values: HashSet<String>, cx: &mut Context<Self>) {
        self.filter_state.set_filter(col_ix, selected_values);
        cx.notify();
    }

    /// Set filter for a column with all values check.
    pub fn set_column_filter_with_all_values(&mut self, col_ix: usize, selected_values: HashSet<String>, cx: &mut Context<Self>) {
        // 获取该列的所有唯一值
        let filter_values = self.delegate.get_column_filter_values(col_ix, cx);
        let all_values: HashSet<String> = filter_values
            .iter()
            .map(|fv| fv.value.to_string())
            .collect();

        self.filter_state.set_filter_with_all_values(col_ix, selected_values, all_values);
        cx.notify();
    }

    /// Clear filter for a column.
    pub fn clear_column_filter(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        self.filter_state.clear_filter(col_ix);
        cx.notify();
    }

    /// Clear all filters.
    pub fn clear_all_filters(&mut self, cx: &mut Context<Self>) {
        self.filter_state.clear_all();
        cx.notify();
    }

    /// 打开筛选面板
    pub fn open_filter_panel(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        let filter_values = self.delegate.get_column_filter_values(col_ix, cx);
        let current_filter = self.filter_state.get_filter(col_ix);
        let values = filter_values
            .into_iter()
            .map(|mut fv| {
                let checked = current_filter
                    .map(|f| f.selected_values.contains(&fv.value))
                    .unwrap_or(false);
                fv.checked = checked;
                fv
            }).collect();

        // 创建FilterPanel并设置实时筛选回调
        let table_entity = cx.entity().clone();
        let filter_panel = FilterPanel::new(values)
            .on_toggle(move |value, window, cx| {
                table_entity.update(cx, |table, cx| {
                    // 使用实时筛选方法，立即应用筛选
                    table.toggle_filter_value_realtime(col_ix, value, window, cx);
                });
            });

        // 创建ListState并包装FilterPanel
        self.filter_list = Some(cx.new(|cx| ListState::new(filter_panel, window, cx).searchable(true)));
        self.active_filter_col = Some(col_ix);
        cx.notify();
    }

    /// 关闭筛选面板
    pub fn close_filter_panel(&mut self, cx: &mut Context<Self>) {
        self.active_filter_col = None;
        self.filter_list = None;
        cx.notify();
    }

    /// 切换筛选面板中的值（实时应用筛选）
    pub fn toggle_filter_value_realtime(&mut self, col_ix: usize, value: &str, window: &mut Window, cx: &mut Context<Self>) {
        // 更新 FilterPanel 并立即应用筛选
        self.modify_filter_panel_realtime(col_ix, window, cx, |panel| {
            panel.toggle_value(value);
        });
    }

    /// 全选筛选项（实时应用筛选）
    pub fn filter_panel_select_all_realtime(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        // 更新 FilterPanel 并立即应用筛选
        self.modify_filter_panel_realtime(col_ix, window, cx, |panel| {
            panel.select_all();
        });
    }

    /// 清空筛选项（实时应用筛选）
    pub fn filter_panel_deselect_all_realtime(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        // 更新 FilterPanel 并立即应用筛选
        self.modify_filter_panel_realtime(col_ix, window, cx, |panel| {
            panel.deselect_all();
        });
    }

    /// 重置筛选（实时应用筛选）
    pub fn filter_panel_reset_realtime(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        // 1. 重置 FilterPanel 为全选状态
        self.update_filter_panel(cx, |panel| {
            panel.select_all();
        });
        // 3. 立即应用筛选
        self.apply_filter_realtime(col_ix, window, cx);
    }

    /// 立即应用筛选（从 FilterPanel 获取当前选中的值）
    fn apply_filter_realtime(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(selected) = self.read_filter_panel_with_ctx(cx, |panel| panel.get_selected_values()) {
            if selected.is_empty() {
                self.clear_column_filter(col_ix, cx);
                // 通知 delegate 筛选已清除
                self.delegate.on_column_filter_cleared(col_ix, window, cx);
            } else {
                self.set_column_filter_with_all_values(col_ix, selected.clone(), cx);
                // 通知 delegate 筛选已变更
                self.delegate.on_column_filter_changed(col_ix, selected, window, cx);
            }

            // 刷新表格以应用筛选
            self.refresh(cx);
        }
    }

    /// 切换筛选面板中的值（旧方法，保留向后兼容）
    pub fn toggle_filter_value(&mut self, value: &str, cx: &mut Context<Self>) {
        self.update_filter_panel(cx, |panel| {
            panel.toggle_value(value);
        });
    }

    /// 在筛选面板中全选
    pub fn filter_panel_select_all(&mut self, cx: &mut Context<Self>) {
        self.update_filter_panel(cx, |panel| {
            panel.select_all();
        });
    }

    /// 在筛选面板中清空选择
    pub fn filter_panel_deselect_all(&mut self, cx: &mut Context<Self>) {
        self.update_filter_panel(cx, |panel| {
            panel.deselect_all();
        });
    }

    /// 重置筛选面板为全选状态
    pub fn filter_panel_reset(&mut self, cx: &mut Context<Self>) {
        self.update_filter_panel(cx, |panel| {
            panel.select_all();
        });
        cx.notify();
    }

    /// 确认筛选面板的选择
    pub fn confirm_filter_panel(&mut self, cx: &mut Context<Self>) {
        if let Some(col_ix) = self.active_filter_col {
            if let Some(selected) = self.read_filter_panel_with_ctx(cx, |panel| panel.get_selected_values()) {
                if selected.is_empty() {
                    self.clear_column_filter(col_ix, cx);
                } else {
                    self.set_column_filter(col_ix, selected, cx);
                }
            }

            self.close_filter_panel(cx);
        }
    }

    /// 获取筛选面板中选中的值数量
    pub fn filter_panel_selected_count(&self, cx: &App) -> usize {
        self.read_filter_panel_with_app(cx, |panel| panel.get_selected_values().len())
            .unwrap_or(0)
    }

    /// 获取筛选面板中的总值数量
    pub fn filter_panel_total_count(&self, cx: &App) -> usize {
        self.read_filter_panel_with_app(cx, |panel| panel.values.len())
            .unwrap_or(0)
    }

    fn modify_filter_panel_realtime<F>(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>, f: F)
    where
        F: FnOnce(&mut FilterPanel),
    {
        let mut action = Some(f);
        self.update_filter_panel(cx, move |panel| {
            if let Some(func) = action.take() {
                func(panel);
            }
        });

        self.apply_filter_realtime(col_ix, window, cx);
    }

    /// 内部方法：更新筛选面板委托
    fn update_filter_panel<F>(&mut self, cx: &mut Context<Self>, mut f: F)
    where
        F: FnMut(&mut FilterPanel),
    {
        if let Some(filter_list) = &self.filter_list {
            filter_list.update(cx, |list_state, cx| {
                f(list_state.delegate_mut());
                cx.notify();
            });
        }
    }

    /// 内部方法：在 Table 上下文中读取筛选面板
    fn read_filter_panel_with_ctx<R, F>(&self, cx: &mut Context<Self>, f: F) -> Option<R>
    where
        F: FnOnce(&FilterPanel) -> R,
    {
        self.filter_list.as_ref().map(|filter_list| {
            filter_list.read_with(cx, |list_state: &ListState<FilterPanel>, _| f(list_state.delegate()))
        })
    }

    /// 内部方法：在 App 上下文中读取筛选面板
    fn read_filter_panel_with_app<R, F>(&self, cx: &App, f: F) -> Option<R>
    where
        F: FnOnce(&FilterPanel) -> R,
    {
        self.filter_list.as_ref().map(|filter_list| {
            filter_list.read_with(cx, |list_state: &ListState<FilterPanel>, _| f(list_state.delegate()))
        })
    }

    fn prepare_col_groups(&mut self, cx: &mut Context<Self>) {
        let mut col_groups = Vec::new();

        // Add row number column if enabled
        if self.delegate.row_number_enabled(cx) {
            col_groups.push(ColGroup {
                width: px(60.),
                bounds: Bounds::default(),
                column: Column::new("__row_number__", " ")
                    .width(px(60.))
                    .resizable(false)
                    .movable(false)
                    .selectable(false)
                    .text_right(),
            });
        }

        // Add user-defined columns
        col_groups.extend((0..self.delegate.columns_count(cx)).map(|col_ix| {
            let column = self.delegate().column(col_ix, cx);
            ColGroup {
                width: column.width,
                bounds: Bounds::default(),
                column: column.clone(),
            }
        }));

        self.col_groups = col_groups;
        cx.notify();
    }

    fn fixed_left_cols_count(&self) -> usize {
        if !self.col_fixed {
            return 0;
        }

        self.col_groups
            .iter()
            .filter(|col| col.column.fixed == Some(ColumnFixed::Left))
            .count()
    }

    fn on_row_right_click(
        &mut self,
        _: &MouseDownEvent,
        row_ix: usize,
        _: &mut Window,
        _: &mut Context<Self>,
    ) {
        self.right_clicked_row = Some(row_ix);
    }

    fn on_row_left_click(
        &mut self,
        _e: &ClickEvent,
        row_ix: usize,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_selected_row(row_ix, cx);
    }

    fn on_cell_click(
        &mut self,
        e: &ClickEvent,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // If clicking on a different cell while editing, commit current edit first
        if let Some((edit_row, edit_col)) = self.editing_cell {
            if edit_row != row_ix || edit_col != col_ix {
                self.commit_cell_edit(window, cx);
            }
        }

        // Check if this is the row number column
        let is_row_number_col = self.delegate.row_number_enabled(cx) && col_ix == 0;

        if e.click_count() == 2 {
            // Double click: enter edit mode (not for row number column)
            if !is_row_number_col {
                self.start_editing(row_ix, col_ix, window, cx);
            }
        } else {
            // Single click
            if !is_row_number_col {
                // Click on row number column: select entire row
                self.set_selected_cell(row_ix, col_ix, cx);
            }
        }
    }

    /// Start editing a cell.
    pub fn start_editing(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {

        if self.editing_cell == Some((row_ix, col_ix)) {
            return;
        }
        // Calculate the actual column index for delegate
        let delegate_col_ix = if self.delegate.row_number_enabled(cx) {
            col_ix.saturating_sub(1)
        } else {
            col_ix
        };

        let input = self.delegate.build_input(row_ix, delegate_col_ix, window, cx);
        if input.is_some() {
            self.editing_cell = Some((row_ix, col_ix));
            let (input, _sub) = input.unwrap();
            self.editing_input = Some(input);
            self._sub = Some(_sub);
            cx.emit(TableEvent::CellEditing(row_ix, col_ix));
            cx.notify();
        }
    }

    /// Commit the current cell edit.
    pub fn commit_cell_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some((row_ix, col_ix)) = self.editing_cell {
            // Calculate the actual column index for delegate
            let delegate_col_ix = if self.delegate.row_number_enabled(cx) {
                col_ix.saturating_sub(1)
            } else {
                col_ix
            };

            // Get the new value from the input
            let new_value = self
                .editing_input
                .as_ref()
                .map(|input| input.read(cx).text().to_string())
                .unwrap_or_default();

            // Call delegate to handle the edit
            let accepted = self
                .delegate
                .on_cell_edited(row_ix, delegate_col_ix, new_value, window, cx);
            if accepted {
                cx.emit(TableEvent::CellEdited(row_ix, col_ix));
            }
            self.editing_cell = None;
            self.editing_input = None;
            self._sub = None;
            cx.notify();
        }
    }

    /// Cancel the current cell edit.
    pub fn cancel_cell_edit(&mut self, cx: &mut Context<Self>) {
        if self.editing_cell.is_some() {
            self.editing_cell = None;
            self.editing_input = None;
            self._sub = None;
            cx.notify();
        }
    }

    /// Returns the editing input state if currently editing.
    pub fn editing_input(&self) -> Option<&Entity<InputState>> {
        self.editing_input.as_ref()
    }

    /// Returns the currently editing cell position.
    pub fn editing_cell(&self) -> Option<(usize, usize)> {
        self.editing_cell
    }

    /// Add a new row.
    pub fn add_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let row_ix = self.delegate.on_row_added(window, cx);
        self.scroll_to_row(row_ix, cx);
        cx.emit(TableEvent::RowAdded);
        self.refresh(cx);
        cx.notify();
    }

    /// Delete a row.
    pub fn delete_row(&mut self, row_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        self.delegate.on_row_deleted(row_ix, window, cx);
        cx.emit(TableEvent::RowDeleted(row_ix));
        if self.selected_row == Some(row_ix) {
            self.selected_row = None;
        }
        self.refresh(cx);
    }

    fn on_col_head_click(&mut self, col_ix: usize, _: &mut Window, cx: &mut Context<Self>) {
        if !self.col_selectable {
            return;
        }

        let Some(col_group) = self.col_groups.get(col_ix) else {
            return;
        };

        if !col_group.column.selectable {
            return;
        }

        self.set_selected_col(col_ix, cx)
    }

    fn has_selection(&self) -> bool {
        self.selected_row.is_some() || self.selected_col.is_some()
    }

    pub(super) fn action_confirm(&mut self, _: &Confirm, window: &mut Window, cx: &mut Context<Self>) {
        // Commit editing if in edit mode
        if self.editing_cell.is_some() {
            self.commit_cell_edit(window, cx);
        }
    }

    pub(super) fn action_cancel(&mut self, _: &Cancel, _: &mut Window, cx: &mut Context<Self>) {
        // Cancel editing first if in edit mode
        if self.editing_cell.is_some() {
            self.cancel_cell_edit(cx);
            return;
        }

        if self.has_selection() {
            self.clear_selection(cx);
            return;
        }
        cx.propagate();
    }

    pub(super) fn action_select_prev(
        &mut self,
        _: &SelectUp,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let rows_count = self.delegate.rows_count(cx);
        if rows_count < 1 {
            return;
        }

        let mut selected_row = self.selected_row.unwrap_or(0);
        if selected_row > 0 {
            selected_row = selected_row.saturating_sub(1);
        } else {
            if self.loop_selection {
                selected_row = rows_count.saturating_sub(1);
            }
        }

        self.set_selected_row(selected_row, cx);
    }

    pub(super) fn action_select_next(
        &mut self,
        _: &SelectDown,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let rows_count = self.delegate.rows_count(cx);
        if rows_count < 1 {
            return;
        }

        let selected_row = match self.selected_row {
            Some(selected_row) if selected_row < rows_count.saturating_sub(1) => selected_row + 1,
            Some(selected_row) => {
                if self.loop_selection {
                    0
                } else {
                    selected_row
                }
            }
            _ => 0,
        };

        self.set_selected_row(selected_row, cx);
    }

    pub(super) fn action_select_prev_col(
        &mut self,
        _: &SelectPrevColumn,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut selected_col = self.selected_col.unwrap_or(0);
        let columns_count = self.delegate.columns_count(cx);
        if selected_col > 0 {
            selected_col = selected_col.saturating_sub(1);
        } else {
            if self.loop_selection {
                selected_col = columns_count.saturating_sub(1);
            }
        }
        self.set_selected_col(selected_col, cx);
    }

    pub(super) fn action_select_next_col(
        &mut self,
        _: &SelectNextColumn,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut selected_col = self.selected_col.unwrap_or(0);
        if selected_col < self.delegate.columns_count(cx).saturating_sub(1) {
            selected_col += 1;
        } else {
            if self.loop_selection {
                selected_col = 0;
            }
        }

        self.set_selected_col(selected_col, cx);
    }

    /// Scroll table when mouse position is near the edge of the table bounds.
    fn scroll_table_by_col_resizing(
        &mut self,
        mouse_position: Point<Pixels>,
        col_group: &ColGroup,
    ) {
        // Do nothing if pos out of the table bounds right for avoid scroll to the right.
        if mouse_position.x > self.bounds.right() {
            return;
        }

        let mut offset = self.horizontal_scroll_handle.offset();
        let col_bounds = col_group.bounds;

        if mouse_position.x < self.bounds.left()
            && col_bounds.right() < self.bounds.left() + px(20.)
        {
            offset.x += px(1.);
        } else if mouse_position.x > self.bounds.right()
            && col_bounds.right() > self.bounds.right() - px(20.)
        {
            offset.x -= px(1.);
        }

        self.horizontal_scroll_handle.set_offset(offset);
    }

    /// The `ix`` is the index of the col to resize,
    /// and the `size` is the new size for the col.
    fn resize_cols(&mut self, ix: usize, size: Pixels, _: &mut Window, cx: &mut Context<Self>) {
        if !self.col_resizable {
            return;
        }

        const MIN_WIDTH: Pixels = px(10.0);
        const MAX_WIDTH: Pixels = px(1200.0);
        let Some(col_group) = self.col_groups.get_mut(ix) else {
            return;
        };

        if !col_group.is_resizable() {
            return;
        }
        let size = size.floor();

        let old_width = col_group.width;
        let new_width = size;
        if new_width < MIN_WIDTH {
            return;
        }
        let changed_width = new_width - old_width;
        // If change size is less than 1px, do nothing.
        if changed_width > px(-1.0) && changed_width < px(1.0) {
            return;
        }
        col_group.width = new_width.min(MAX_WIDTH);

        cx.notify();
    }

    fn perform_sort(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        if !self.sortable {
            return;
        }

        let sort = self.col_groups.get(col_ix).and_then(|g| g.column.sort);
        if sort.is_none() {
            return;
        }

        let sort = sort.unwrap();
        let sort = match sort {
            ColumnSort::Ascending => ColumnSort::Default,
            ColumnSort::Descending => ColumnSort::Ascending,
            ColumnSort::Default => ColumnSort::Descending,
        };

        for (ix, col_group) in self.col_groups.iter_mut().enumerate() {
            if ix == col_ix {
                col_group.column.sort = Some(sort);
            } else {
                if col_group.column.sort.is_some() {
                    col_group.column.sort = Some(ColumnSort::Default);
                }
            }
        }

        // Calculate the actual column index for delegate
        let delegate_col_ix = if self.delegate.row_number_enabled(cx) {
            col_ix.saturating_sub(1)
        } else {
            col_ix
        };

        self.delegate_mut().perform_sort(delegate_col_ix, sort, window, cx);

        cx.notify();
    }

    fn move_column(
        &mut self,
        col_ix: usize,
        to_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if col_ix == to_ix {
            return;
        }

        // Don't allow moving the row number column
        let row_number_offset = if self.delegate.row_number_enabled(cx) { 1 } else { 0 };
        if row_number_offset > 0 && (col_ix == 0 || to_ix == 0) {
            return;
        }

        // Calculate the actual column indices for delegate
        let delegate_col_ix = col_ix.saturating_sub(row_number_offset);
        let delegate_to_ix = to_ix.saturating_sub(row_number_offset);

        self.delegate.move_column(delegate_col_ix, delegate_to_ix, window, cx);
        let col_group = self.col_groups.remove(col_ix);
        self.col_groups.insert(to_ix, col_group);

        cx.emit(TableEvent::MoveColumn(col_ix, to_ix));
        cx.notify();
    }

    /// Dispatch delegate's `load_more` method when the visible range is near the end.
    fn load_more_if_need(
        &mut self,
        rows_count: usize,
        visible_end: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let threshold = self.delegate.load_more_threshold();
        // Securely handle subtract logic to prevent attempt to subtract with overflow
        if visible_end >= rows_count.saturating_sub(threshold) {
            if !self.delegate.has_more(cx) {
                return;
            }

            self._load_more_task = cx.spawn_in(window, async move |view, window| {
                _ = view.update_in(window, |view, window, cx| {
                    view.delegate.load_more(window, cx);
                });
            });
        }
    }

    fn update_visible_range_if_need(
        &mut self,
        visible_range: Range<usize>,
        axis: Axis,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Skip when visible range is only 1 item.
        // The visual_list will use first item to measure.
        if visible_range.len() <= 1 {
            return;
        }

        if axis == Axis::Vertical {
            if self.visible_range.rows == visible_range {
                return;
            }
            self.delegate_mut()
                .visible_rows_changed(visible_range.clone(), window, cx);
            self.visible_range.rows = visible_range;
        } else {
            if self.visible_range.cols == visible_range {
                return;
            }
            self.delegate_mut()
                .visible_columns_changed(visible_range.clone(), window, cx);
            self.visible_range.cols = visible_range;
        }
    }

    fn render_cell(
        &self,
        col_ix: usize,
        row_ix: Option<usize>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let Some(col_group) = self.col_groups.get(col_ix) else {
            return div().id("empty-cell");
        };
        let is_select_cell = match self.selected_cell {
            None => false,
            Some(cell) => row_ix.is_some() && row_ix.unwrap() == cell.0 && col_ix == cell.1
        };
        let col_width = col_group.width;
        let col_padding = col_group.column.paddings;

        // Check if cell is modified (skip for row number column)
        let is_row_number_col = self.delegate.row_number_enabled(cx) && col_ix == 0;
        let is_modified = if is_row_number_col {
            false
        } else {
            row_ix
                .map(|r| {
                    let delegate_col_ix = if self.delegate.row_number_enabled(cx) {
                        col_ix - 1
                    } else {
                        col_ix
                    };
                    self.delegate.is_cell_modified(r, delegate_col_ix, cx)
                })
                .unwrap_or(false)
        };

        // Generate unique id: for cells use row*10000+col, for headers use col
        let cell_id = match row_ix {
            Some(r) => ("cell", r * 10000 + col_ix),
            None => ("cell-header", col_ix),
        };

        // Check if this cell is being edited
        let is_editing = row_ix.is_some() && self.editing_cell == Some((row_ix.unwrap(), col_ix));

        let mut cell = div()
            .id(cell_id)
            .w(col_width)
            .h_full()
            .flex_shrink_0()
            .overflow_hidden()
            .whitespace_nowrap()
            .when(is_select_cell, |this| {
                this.bg(if is_editing { cx.theme().background} else {cx.theme().table_active})
                    .border_1()
                    .border_color(if is_editing { cx.theme().ring } else {cx.theme().table_active_border})
            })
            .when(is_modified && !is_editing, |this| {
                this.bg(cx.theme().warning.opacity(0.15))
            });

        if is_editing {
            // Render input box that fills the entire cell
            // Don't apply table_cell_size padding in edit mode
            if let Some(input) = &self.editing_input {
                cell = cell.child(Input::new(input).w_full().h_full().text_base().appearance(false));
            }
        } else {
            // Apply table_cell_size and padding for normal cell content
            cell = cell.table_cell_size(self.options.size);
            cell = match col_padding {
                Some(padding) => cell
                    .pl(padding.left)
                    .pr(padding.right)
                    .pt(padding.top)
                    .pb(padding.bottom),
                None => cell,
            };
        }

        cell
    }

    /// Show Column selection style, when the column is selected and the selection state is Column.
    fn render_col_wrap(&self, col_ix: usize, _: &mut Window, cx: &mut Context<Self>) -> Div {
        let el = h_flex().h_full();
        let selectable = self.col_selectable
            && self
                .col_groups
                .get(col_ix)
                .map(|col_group| col_group.column.selectable)
                .unwrap_or(false);

        if selectable
            && self.selected_col == Some(col_ix)
            && self.selection_state == SelectionState::Column
        {
            el.bg(cx.theme().table_active)
        } else {
            el
        }
    }

    fn render_resize_handle(
        &self,
        ix: usize,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        const HANDLE_SIZE: Pixels = px(2.);

        let resizable = self.col_resizable
            && self
                .col_groups
                .get(ix)
                .map(|col| col.is_resizable())
                .unwrap_or(false);
        if !resizable {
            return div().into_any_element();
        }

        let group_id = SharedString::from(format!("resizable-handle:{}", ix));

        h_flex()
            .id(("resizable-handle", ix))
            .group(group_id.clone())
            .occlude()
            .cursor_col_resize()
            .h_full()
            .w(HANDLE_SIZE)
            .ml(-(HANDLE_SIZE))
            .justify_end()
            .items_center()
            .child(
                div()
                    .h_full()
                    .justify_center()
                    .bg(cx.theme().table_row_border)
                    .group_hover(group_id, |this| this.bg(cx.theme().border).h_full())
                    .w(px(1.)),
            )
            .on_drag_move(
                cx.listener(move |view, e: &DragMoveEvent<ResizeColumn>, window, cx| {
                    match e.drag(cx) {
                        ResizeColumn((entity_id, ix)) => {
                            if cx.entity_id() != *entity_id {
                                return;
                            }

                            // sync col widths into real widths
                            // TODO: Consider to remove this, this may not need now.
                            // for (_, col_group) in view.col_groups.iter_mut().enumerate() {
                            //     col_group.width = col_group.bounds.size.width;
                            // }

                            let ix = *ix;
                            view.resizing_col = Some(ix);

                            let col_group = view
                                .col_groups
                                .get(ix)
                                .expect("BUG: invalid col index")
                                .clone();

                            view.resize_cols(
                                ix,
                                e.event.position.x - HANDLE_SIZE - col_group.bounds.left(),
                                window,
                                cx,
                            );

                            // scroll the table if the drag is near the edge
                            view.scroll_table_by_col_resizing(e.event.position, &col_group);
                        }
                    };
                }),
            )
            .on_drag(ResizeColumn((cx.entity_id(), ix)), |drag, _, _, cx| {
                cx.stop_propagation();
                cx.new(|_| drag.clone())
            })
            .on_mouse_up_out(
                MouseButton::Left,
                cx.listener(|view, _, _, cx| {
                    if view.resizing_col.is_none() {
                        return;
                    }

                    view.resizing_col = None;

                    let new_widths = view.col_groups.iter().map(|g| g.width).collect();
                    cx.emit(TableEvent::ColumnWidthsChanged(new_widths));
                    cx.notify();
                }),
            )
            .into_any_element()
    }

    fn render_sort_icon(
        &self,
        col_ix: usize,
        col_group: &ColGroup,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        if !self.sortable {
            return None;
        }

        let Some(sort) = col_group.column.sort else {
            return None;
        };

        let (icon, is_on) = match sort {
            ColumnSort::Ascending => (IconName::SortAscending, true),
            ColumnSort::Descending => (IconName::SortDescending, true),
            ColumnSort::Default => (IconName::ChevronsUpDown, false),
        };

        Some(
            div()
                .id(("icon-sort", col_ix))
                .p(px(2.))
                .rounded(cx.theme().radius / 2.)
                .map(|this| match is_on {
                    true => this,
                    false => this.opacity(0.5),
                })
                .hover(|this| this.bg(cx.theme().secondary).opacity(7.))
                .active(|this| this.bg(cx.theme().secondary_active).opacity(1.))
                .on_click(
                    cx.listener(move |table, _, window, cx| table.perform_sort(col_ix, window, cx)),
                )
                .child(
                    Icon::new(icon)
                        .size_3()
                        .text_color(cx.theme().secondary_foreground),
                ),
        )
    }

    fn render_filter_icon(
        &self,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        if !self.col_filterable || !self.delegate.column_filter_enabled(cx) {
            return None;
        }

        let is_filtered = self.filter_state.is_column_filtered(col_ix);
        let is_open = self.active_filter_col == Some(col_ix);
        let table_entity = cx.entity().clone();

        use crate::{button::Button, button::ButtonVariants, popover::Popover, Sizable, Size};

        let filter_content = if is_open {
            Some(self.render_filter_panel_content(col_ix, _window, cx))
        } else {
            None
        };

        Some(
            Popover::new(("filter-popover", col_ix))
                .trigger(
                    Button::new(("filter-btn", col_ix))
                        .icon(IconName::Filter)
                        .ghost()
                        .with_size(Size::XSmall)
                        .when(is_filtered, |this| this.primary())
                )
                .open(is_open)
                .on_open_change({
                    let entity = table_entity.clone();
                    move |open, window, cx| {
                        entity.update(cx, |table, cx| {
                            if *open {
                                table.open_filter_panel(col_ix, window, cx);
                            } else {
                                table.close_filter_panel(cx);
                            }
                        });
                    }
                })
                .p_0()
                .children(filter_content)
        )
    }

    fn render_filter_panel_content(
        &self,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        use crate::{button::Button, button::ButtonVariants, Sizable, Size};

        let table_entity = cx.entity().clone();
        let selected_count = self.filter_panel_selected_count(cx);
        let total_count = self.filter_panel_total_count(cx);

        let filter_list = match &self.filter_list {
            Some(list) => list.clone(),
            None => return div().into_any_element(),
        };

        v_flex()
            .w(px(280.))
            .max_h(px(400.))
            .gap_2()
            .p_2()
            // 统计行
            .child(
                h_flex()
                    .w_full()
                    .justify_between()
                    .items_center()
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("已选 {} / {}", selected_count, total_count)),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("filter-select-all")
                                    .label("全选")
                                    .ghost()
                                    .with_size(Size::XSmall)
                                    .on_click({
                                        let entity = table_entity.clone();
                                        move |_, window, cx| {
                                            entity.update(cx, |table, cx| {
                                                table.filter_panel_select_all_realtime(col_ix, window, cx);
                                            });
                                        }
                                    }),
                            )
                            .child(
                                Button::new("filter-deselect-all")
                                    .label("清除筛选")
                                    .ghost()
                                    .with_size(Size::XSmall)
                                    .on_click({
                                        let entity = table_entity.clone();
                                        move |_, window, cx| {
                                            entity.update(cx, |table, cx| {
                                                table.filter_panel_deselect_all_realtime(col_ix, window, cx);
                                            });
                                        }
                                    }),
                            ),
                    ),
            )
            // 分隔线
            .child(div().h(px(1.)).w_full().bg(cx.theme().border))
            // 列表
            .child(
                List::new(&filter_list)
                    .max_h(px(200.))
                    .p(px(8.))
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded(cx.theme().radius)
            )
            .into_any_element()
    }

    /// Render the column header.
    /// The children must be one by one items.
    /// Because the horizontal scroll handle will use the child_item_bounds to
    /// calculate the item position for itself's `scroll_to_item` method.
    fn render_th(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) -> Div {
        let entity_id = cx.entity_id();
        let col_group = self.col_groups.get(col_ix).expect("BUG: invalid col index");

        let is_row_number_col = self.delegate.row_number_enabled(cx) && col_ix == 0;
        let movable = self.col_movable && col_group.column.movable && !is_row_number_col;
        let paddings = col_group.column.paddings;
        let name = col_group.column.name.clone();
        // Calculate the actual column index for delegate
        let delegate_col_ix = if self.delegate.row_number_enabled(cx) && col_ix > 0 {
            col_ix - 1
        } else {
            col_ix
        };

        h_flex()
            .h_full()
            .child(
                self.render_cell(col_ix, None, window, cx)
                    .id(("col-header", col_ix))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.on_col_head_click(col_ix, window, cx);
                    }))
                    .child(
                        h_flex()
                            .size_full()
                            .justify_between()
                            .items_center()
                            .child(
                                if is_row_number_col {
                                    // Render row number column header
                                    div()
                                        .size_full()
                                        .flex()
                                        .items_center()
                                        .justify_end()
                                        .child(col_group.column.name.clone())
                                        .into_any_element()
                                } else {

                                    self.delegate.render_th(delegate_col_ix, window, cx).into_any_element()
                                }
                            )
                            .when_some(paddings, |this, paddings| {
                                // Leave right space for the sort/filter icons, if this column have custom padding
                                let offset_pr =
                                    self.options.size.table_cell_padding().right - paddings.right;
                                this.pr(offset_pr.max(px(0.)))
                            })
                            .when(!is_row_number_col, |this| {
                                this.children(self.render_filter_icon(delegate_col_ix, window, cx))
                            })
                            .when(!is_row_number_col, |this| {
                                this.children(self.render_sort_icon(delegate_col_ix,&col_group, window, cx))
                            })
                    )
                    .when(movable, |this| {
                        this.on_drag(
                            DragColumn {
                                entity_id,
                                col_ix: delegate_col_ix,
                                name,
                                width: col_group.width,
                            },
                            |drag, _, _, cx| {
                                cx.stop_propagation();
                                cx.new(|_| drag.clone())
                            },
                        )
                        .drag_over::<DragColumn>(|this, _, _, cx| {
                            this.rounded_l_none()
                                .border_l_2()
                                .border_r_0()
                                .border_color(cx.theme().drag_border)
                        })
                        .on_drop(cx.listener(
                            move |table, drag: &DragColumn, window, cx| {
                                // If the drag col is not the same as the drop col, then swap the cols.
                                if drag.entity_id != cx.entity_id() {
                                    return;
                                }

                                table.move_column(drag.col_ix, delegate_col_ix, window, cx);
                            },
                        ))
                    }),
            )
            // resize handle
            .child(self.render_resize_handle(delegate_col_ix, window, cx))
            // to save the bounds of this col.
            .child({
                let view = cx.entity().clone();
                canvas(
                    move |bounds, _, cx| {
                        view.update(cx, |r, _| r.col_groups[col_ix].bounds = bounds)
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full()
            })
    }

    fn render_table_header(
        &mut self,
        left_columns_count: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let view = cx.entity().clone();
        let horizontal_scroll_handle = self.horizontal_scroll_handle.clone();

        // Reset fixed head columns bounds, if no fixed columns are present
        if left_columns_count == 0 {
            self.fixed_head_cols_bounds = Bounds::default();
        }

        let mut header = self.delegate_mut().render_header(window, cx);
        let style = header.style().clone();

        header
            .h_flex()
            .w_full()
            .h(self.options.size.table_row_height())
            .flex_shrink_0()
            .border_b_1()
            .border_color(cx.theme().border)
            .text_color(cx.theme().table_head_foreground)
            .refine_style(&style)
            .when(left_columns_count > 0, |this| {
                let view = view.clone();
                // Render left fixed columns
                this.child(
                    h_flex()
                        .relative()
                        .h_full()
                        .bg(cx.theme().table_head)
                        .children(
                            self.col_groups
                                .clone()
                                .into_iter()
                                .filter(|col| col.column.fixed == Some(ColumnFixed::Left))
                                .enumerate()
                                .map(|(col_ix, _)| self.render_th(col_ix, window, cx)),
                        )
                        .child(
                            // Fixed columns border
                            div()
                                .absolute()
                                .top_0()
                                .right_0()
                                .bottom_0()
                                .w_0()
                                .flex_shrink_0()
                                .border_r_1()
                                .border_color(cx.theme().border),
                        )
                        .child(
                            canvas(
                                move |bounds, _, cx| {
                                    view.update(cx, |r, _| r.fixed_head_cols_bounds = bounds)
                                },
                                |_, _, _, _| {},
                            )
                            .absolute()
                            .size_full(),
                        ),
                )
            })
            .child(
                // Columns
                h_flex()
                    .id("table-head")
                    .size_full()
                    .overflow_scroll()
                    .relative()
                    .track_scroll(&horizontal_scroll_handle)
                    .bg(cx.theme().table_head)
                    .child(
                        h_flex()
                            .relative()
                            .children(
                                self.col_groups
                                    .clone()
                                    .into_iter()
                                    .skip(left_columns_count)
                                    .enumerate()
                                    .map(|(col_ix, _)| {
                                        self.render_th(left_columns_count + col_ix, window, cx)
                                    }),
                            )
                            .child(self.delegate.render_last_empty_col(window, cx)),
                    ),
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_table_row(
        &mut self,
        row_ix: usize,
        rows_count: usize,
        left_columns_count: usize,
        col_sizes: Rc<Vec<gpui::Size<Pixels>>>,
        columns_count: usize,
        is_filled: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let horizontal_scroll_handle = self.horizontal_scroll_handle.clone();
        let is_stripe_row = self.options.stripe && row_ix % 2 != 0;
        let is_selected = self.selected_row == Some(row_ix);
        let view = cx.entity().clone();
        let row_height = self.options.size.table_row_height();

        if row_ix < rows_count {
            let is_last_row = row_ix + 1 == rows_count;
            let need_render_border = is_selected || !is_last_row || !is_filled;

            let mut tr = self.delegate.render_tr(row_ix, window, cx);
            let style = tr.style().clone();

            tr.h_flex()
                .w_full()
                .h(row_height)
                .when(need_render_border, |this| {
                    this.border_b_1().border_color(cx.theme().table_row_border)
                })
                .when(is_stripe_row, |this| this.bg(cx.theme().table_even))
                .refine_style(&style)
                .hover(|this| {
                    if is_selected || self.right_clicked_row == Some(row_ix) {
                        this
                    } else {
                        this.bg(cx.theme().table_hover)
                    }
                })
                .when(left_columns_count > 0, |this| {
                    // Left fixed columns
                    this.child(
                        h_flex()
                            .relative()
                            .h_full()
                            .children({
                                let mut items = Vec::with_capacity(left_columns_count);

                                (0..left_columns_count).for_each(|col_ix| {
                                    let is_editing = self.editing_cell == Some((row_ix, col_ix));
                                    items.push(
                                        self.render_col_wrap(col_ix, window, cx).child(
                                            self.render_cell(col_ix, Some(row_ix), window, cx)
                                                .on_click(cx.listener(
                                                    move |this, e, window, cx| {
                                                        this.on_cell_click(
                                                            e, row_ix, col_ix, window, cx,
                                                        );
                                                    },
                                                ))
                                                .when(!is_editing, |this| {
                                                    this.child(
                                                        self.measure_render_td(
                                                            row_ix, col_ix, window, cx,
                                                        ),
                                                    )
                                                }),
                                        ),
                                    );
                                });

                                items
                            })
                            .child(
                                // Fixed columns border
                                div()
                                    .absolute()
                                    .top_0()
                                    .right_0()
                                    .bottom_0()
                                    .w_0()
                                    .flex_shrink_0()
                                    .border_r_1()
                                    .border_color(cx.theme().border),
                            ),
                    )
                })
                .child(
                    h_flex()
                        .flex_1()
                        .h_full()
                        .overflow_hidden()
                        .relative()
                        .child(
                            crate::virtual_list::virtual_list(
                                view,
                                row_ix,
                                Axis::Horizontal,
                                col_sizes,
                                {
                                    move |table, visible_range: Range<usize>, window, cx| {
                                        table.update_visible_range_if_need(
                                            visible_range.clone(),
                                            Axis::Horizontal,
                                            window,
                                            cx,
                                        );

                                        let mut items = Vec::with_capacity(
                                            visible_range.end - visible_range.start,
                                        );

                                        visible_range.for_each(|col_ix| {
                                            let col_ix = col_ix + left_columns_count;
                                            let is_editing = table.editing_cell == Some((row_ix, col_ix));
                                            let el = table
                                                .render_col_wrap(col_ix, window, cx)
                                                .child(
                                                    table
                                                        .render_cell(col_ix, Some(row_ix), window, cx)
                                                        .on_click(cx.listener(
                                                            move |this, e, window, cx| {
                                                                this.on_cell_click(
                                                                    e, row_ix, col_ix, window, cx,
                                                                );
                                                            },
                                                        ))
                                                        .when(!is_editing, |this| {
                                                            this.child(table.measure_render_td(
                                                                row_ix, col_ix, window, cx,
                                                            ))
                                                        }),
                                                );

                                            items.push(el);
                                        });

                                        items
                                    }
                                },
                            )
                            .with_scroll_handle(&self.horizontal_scroll_handle),
                        )
                        .child(self.delegate.render_last_empty_col(window, cx)),
                )
                // Row selected style
                .when_some(self.selected_row, |this, _| {
                    this.when(
                        is_selected && self.selection_state == SelectionState::Row,
                        |this| {
                            this.border_color(gpui::transparent_white()).child(
                                div()
                                    .top(if row_ix == 0 { px(0.) } else { px(-1.) })
                                    .left(px(0.))
                                    .right(px(0.))
                                    .bottom(px(-1.))
                                    .absolute()
                                    .bg(cx.theme().table_active)
                                    .border_1()
                                    .border_color(cx.theme().table_active_border),
                            )
                        },
                    )
                })
                // Row right click row style
                .when(self.right_clicked_row == Some(row_ix), |this| {
                    this.border_color(gpui::transparent_white()).child(
                        div()
                            .top(if row_ix == 0 { px(0.) } else { px(-1.) })
                            .left(px(0.))
                            .right(px(0.))
                            .bottom(px(-1.))
                            .absolute()
                            .border_1()
                            .border_color(cx.theme().selection),
                    )
                })
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(move |this, e, window, cx| {
                        this.on_row_right_click(e, row_ix, window, cx);
                    }),
                )
                // .on_click(cx.listener(move |this, e, window, cx| {
                //     this.on_row_left_click(e, row_ix, window, cx);
                // }))
        } else {
            // Render fake rows to fill the rest table space
            self.delegate
                .render_tr(row_ix, window, cx)
                .h_flex()
                .w_full()
                .h(row_height)
                .border_b_1()
                .border_color(cx.theme().table_row_border)
                .when(is_stripe_row, |this| this.bg(cx.theme().table_even))
                .children((0..columns_count).map(|col_ix| {
                    h_flex()
                        .left(horizontal_scroll_handle.offset().x)
                        .child(self.render_cell(col_ix, Some(row_ix), window, cx))
                }))
                .child(self.delegate.render_last_empty_col(window, cx))
        }
    }

    /// Calculate the extra rows needed to fill the table empty space when `stripe` is true.
    fn calculate_extra_rows_needed(
        &self,
        total_height: Pixels,
        actual_height: Pixels,
        row_height: Pixels,
    ) -> usize {
        let mut extra_rows_needed = 0;

        let remaining_height = total_height - actual_height;
        if remaining_height > px(0.) {
            extra_rows_needed = (remaining_height / row_height).floor() as usize;
        }

        extra_rows_needed
    }

    #[inline]
    fn measure_render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        // Check if this is the row number column
        let is_row_number_col = self.delegate.row_number_enabled(cx) && col_ix == 0;

        if is_row_number_col {
            // Render row number
            return div()
                .id(ElementId::Name(format!("row-number-{}", row_ix).into()))
                .size_full()
                .flex()
                .items_center()
                .justify_end()
                .text_color(cx.theme().muted_foreground)
                .child((row_ix + 1).to_string())
                .on_click(cx.listener(move |this, e, window, cx| {
                    this.on_row_left_click(e, row_ix, window, cx);
                }))
                .into_any_element();
        }

        // Calculate the actual column index for delegate (subtract row number column offset)
        let delegate_col_ix = if self.delegate.row_number_enabled(cx) {
            col_ix - 1
        } else {
            col_ix
        };

        if !crate::measure_enable() {
            return self
                .delegate
                .render_td(row_ix, delegate_col_ix, window, cx)
                .into_any_element();
        }

        let start = std::time::Instant::now();
        let el = self.delegate
            .render_td(row_ix, delegate_col_ix, window, cx)
            .into_any_element();
        self._measure.push(start.elapsed());
        el
    }

    fn measure(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        if !crate::measure_enable() {
            return;
        }

        // Print avg measure time of each td
        if self._measure.len() > 0 {
            let total = self
                ._measure
                .iter()
                .fold(Duration::default(), |acc, d| acc + *d);
            let avg = total / self._measure.len() as u32;
            eprintln!(
                "last render {} cells total: {:?}, avg: {:?}",
                self._measure.len(),
                total,
                avg,
            );
        }
        self._measure.clear();
    }

    fn render_vertical_scrollbar(
        &mut self,

        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        Some(
            div()
                .occlude()
                .absolute()
                .top(self.options.size.table_row_height())
                .right_0()
                .bottom_0()
                .w(Scrollbar::width())
                .child(Scrollbar::vertical(&self.vertical_scroll_handle).max_fps(60)),
        )
    }

    fn render_horizontal_scrollbar(
        &mut self,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .occlude()
            .absolute()
            .left(self.fixed_head_cols_bounds.size.width)
            .right_0()
            .bottom_0()
            .h(Scrollbar::width())
            .child(Scrollbar::horizontal(&self.horizontal_scroll_handle))
    }
}

impl<D> Focusable for TableState<D>
where
    D: TableDelegate,
{
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
impl<D> EventEmitter<TableEvent> for TableState<D> where D: TableDelegate {}

impl<D> Render for TableState<D>
where
    D: TableDelegate,
{
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.measure(window, cx);

        let columns_count = self.delegate.columns_count(cx);
        let left_columns_count = self
            .col_groups
            .iter()
            .filter(|col| self.col_fixed && col.column.fixed == Some(ColumnFixed::Left))
            .count();
        let rows_count = self.delegate.rows_count(cx);
        let loading = self.delegate.loading(cx);

        let row_height = self.options.size.table_row_height();
        let total_height = self
            .vertical_scroll_handle
            .0
            .borrow()
            .base_handle
            .bounds()
            .size
            .height;
        let actual_height = row_height * rows_count as f32;
        let extra_rows_count =
            self.calculate_extra_rows_needed(total_height, actual_height, row_height);
        let render_rows_count = if self.options.stripe {
            rows_count + extra_rows_count
        } else {
            rows_count
        };
        let right_clicked_row = self.right_clicked_row;
        let is_filled = total_height > Pixels::ZERO && total_height <= actual_height;

        let loading_view = if loading {
            Some(
                self.delegate
                    .render_loading(self.options.size, window, cx)
                    .into_any_element(),
            )
        } else {
            None
        };

        let empty_view = if rows_count == 0 {
            Some(
                div()
                    .size_full()
                    .child(self.delegate.render_empty(window, cx))
                    .into_any_element(),
            )
        } else {
            None
        };

        let inner_table = v_flex()
            .id("table-inner")
            .size_full()
            .overflow_hidden()
            .child(self.render_table_header(left_columns_count, window, cx))
            .context_menu({
                let view = cx.entity().clone();
                move |this, window: &mut Window, cx: &mut Context<PopupMenu>| {
                    if let Some(row_ix) = view.read(cx).right_clicked_row {
                        view.update(cx, |menu, cx| {
                            menu.delegate_mut().context_menu(row_ix, this, window, cx)
                        })
                    } else {
                        this
                    }
                }
            })
            .map(|this| {
                if rows_count == 0 {
                    this.children(empty_view)
                } else {
                    this.child(
                        h_flex().id("table-body").flex_grow().size_full().child(
                            uniform_list(
                                "table-uniform-list",
                                render_rows_count,
                                cx.processor(
                                    move |table, visible_range: Range<usize>, window, cx| {
                                        // We must calculate the col sizes here, because the col sizes
                                        // need render_th first, then that method will set the bounds of each col.
                                        let col_sizes: Rc<Vec<gpui::Size<Pixels>>> = Rc::new(
                                            table
                                                .col_groups
                                                .iter()
                                                .skip(left_columns_count)
                                                .map(|col| col.bounds.size)
                                                .collect(),
                                        );

                                        table.load_more_if_need(
                                            rows_count,
                                            visible_range.end,
                                            window,
                                            cx,
                                        );
                                        table.update_visible_range_if_need(
                                            visible_range.clone(),
                                            Axis::Vertical,
                                            window,
                                            cx,
                                        );

                                        if visible_range.end > rows_count {
                                            table.scroll_to_row(
                                                std::cmp::min(
                                                    visible_range.start,
                                                    rows_count.saturating_sub(1),
                                                ),
                                                cx,
                                            );
                                        }

                                        let mut items = Vec::with_capacity(
                                            visible_range.end.saturating_sub(visible_range.start),
                                        );

                                        // Render fake rows to fill the table
                                        visible_range.for_each(|row_ix| {
                                            // Render real rows for available data
                                            items.push(table.render_table_row(
                                                row_ix,
                                                rows_count,
                                                left_columns_count,
                                                col_sizes.clone(),
                                                columns_count,
                                                is_filled,
                                                window,
                                                cx,
                                            ));
                                        });

                                        items
                                    },
                                ),
                            )
                            .flex_grow()
                            .size_full()
                            .with_sizing_behavior(ListSizingBehavior::Auto)
                            .track_scroll(&self.vertical_scroll_handle)
                            .into_any_element(),
                        ),
                    )
                }
            });

        div()
            .size_full()
            .children(loading_view)
            .when(!loading, |this| {
                this.child(inner_table)
                    .child(ScrollableMask::new(
                        Axis::Horizontal,
                        &self.horizontal_scroll_handle,
                    ))
                    .when(right_clicked_row.is_some(), |this| {
                        this.on_mouse_down_out(cx.listener(|this, _, _, cx| {
                            this.right_clicked_row = None;
                            cx.notify();
                        }))
                    })
            })
            .child(canvas(
                {
                    let state = cx.entity();
                    move |bounds, _, cx| state.update(cx, |state, _| state.bounds = bounds)
                },
                |_, _, _, _| {},
            ))
            .when(!window.is_inspector_picking(cx), |this| {
                this.child(
                    div()
                        .absolute()
                        .top_0()
                        .size_full()
                        .when(self.options.scrollbar_visible.bottom, |this| {
                            this.child(self.render_horizontal_scrollbar(window, cx))
                        })
                        .when(
                            self.options.scrollbar_visible.right && rows_count > 0,
                            |this| this.children(self.render_vertical_scrollbar(window, cx)),
                        ),
                )
            })
    }
}
