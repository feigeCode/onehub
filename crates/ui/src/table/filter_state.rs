use std::collections::{HashMap, HashSet};

// ============================================================================
// Constants
// ============================================================================

/// 唯一值数量阈值：超过此值时显示警告
pub const UNIQUE_VALUE_WARNING_THRESHOLD: usize = 1000;

/// 唯一值数量硬限制：超过此值时只显示前N个
pub const UNIQUE_VALUE_HARD_LIMIT: usize = 5000;

// ============================================================================
// Column Filter - Single column filter state
// ============================================================================

/// 列筛选状态
#[derive(Clone, Debug)]
pub struct ColumnFilter {
    /// 选中的值集合（用于快速查找）
    pub selected_values: HashSet<String>,
    /// 是否激活（当所有值都选中时视为未激活）
    pub is_active: bool,
}

impl ColumnFilter {
    /// 创建新的列筛选
    pub fn new(selected_values: HashSet<String>, is_active: bool) -> Self {
        Self {
            selected_values,
            is_active,
        }
    }
}

// ============================================================================
// Filter State - Global filter state manager
// ============================================================================

/// 表格筛选状态
#[derive(Clone, Debug)]
pub struct FilterState {
    /// 列索引 -> 筛选条件
    filters: HashMap<usize, ColumnFilter>,
    /// 筛选后的行索引
    filtered_row_indices: Vec<usize>,
}

impl FilterState {
    /// 创建新的筛选状态
    pub fn new() -> Self {
        Self {
            filters: HashMap::new(),
            filtered_row_indices: Vec::new(),
        }
    }

    /// 设置列筛选
    pub fn set_filter(&mut self, col_ix: usize, selected_values: HashSet<String>) {
        let is_active = !selected_values.is_empty();
        self.filters.insert(
            col_ix,
            ColumnFilter::new(selected_values, is_active),
        );
    }

    /// 清除列筛选
    pub fn clear_filter(&mut self, col_ix: usize) {
        self.filters.remove(&col_ix);
    }

    /// 清除所有筛选
    pub fn clear_all(&mut self) {
        self.filters.clear();
        self.filtered_row_indices.clear();
    }

    /// 检查列是否有激活的筛选
    pub fn is_column_filtered(&self, col_ix: usize) -> bool {
        self.filters
            .get(&col_ix)
            .map(|f| f.is_active)
            .unwrap_or(false)
    }

    /// 获取列的筛选状态
    pub fn get_filter(&self, col_ix: usize) -> Option<&ColumnFilter> {
        self.filters.get(&col_ix)
    }

    /// 应用筛选，返回筛选后的行索引
    /// 
    /// 使用多列 AND 逻辑：行必须满足所有激活的列筛选条件
    pub fn apply_filters(&mut self, rows: &[Vec<String>]) -> Vec<usize> {
        // 如果没有激活的筛选，返回所有行索引
        if self.filters.is_empty() {
            self.filtered_row_indices = (0..rows.len()).collect();
            return self.filtered_row_indices.clone();
        }

        // 遍历所有行，检查是否满足所有筛选条件
        self.filtered_row_indices = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                // 检查所有激活的列筛选
                self.filters.iter().all(|(col_ix, filter)| {
                    if !filter.is_active {
                        return true;
                    }

                    // 获取单元格值
                    let cell_value = row.get(*col_ix).map(|s| s.as_str()).unwrap_or("NULL");

                    // 检查值是否在选中集合中
                    filter.selected_values.contains(cell_value)
                })
            })
            .map(|(ix, _)| ix)
            .collect();

        self.filtered_row_indices.clone()
    }

    /// 获取筛选后的行数
    pub fn filtered_count(&self) -> usize {
        self.filtered_row_indices.len()
    }

    /// 获取筛选后的行索引
    pub fn filtered_indices(&self) -> &[usize] {
        &self.filtered_row_indices
    }

    /// 获取所有激活的筛选列索引
    pub fn active_filter_columns(&self) -> HashSet<usize> {
        self.filters
            .iter()
            .filter_map(|(col_ix, filter)| {
                if filter.is_active {
                    Some(*col_ix)
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for FilterState {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Filter Panel State - UI state for filter panel
// ============================================================================

/// 筛选面板状态
#[derive(Clone, Debug)]
pub struct FilterPanelState {
    /// 列索引
    col_ix: usize,
    /// 唯一值列表
    unique_values: Vec<UniqueValue>,
    /// 当前选中的值
    selected_values: HashSet<String>,
    /// 搜索关键词
    search_query: String,
    /// 过滤后的唯一值列表
    filtered_unique_values: Vec<UniqueValue>,
    /// 是否显示警告（唯一值过多）
    show_warning: bool,
    /// 是否被截断（超过硬限制）
    is_truncated: bool,
    /// 总唯一值数量（截断前）
    total_unique_count: usize,
}

impl FilterPanelState {
    /// 创建新的筛选面板状态
    pub fn new(
        col_ix: usize,
        rows: &[Vec<String>],
        current_filter: Option<&ColumnFilter>,
    ) -> Self {
        let (unique_values, total_count, is_truncated) = extract_unique_values_with_limit(col_ix, rows);

        // 检查是否需要显示警告
        let show_warning = total_count >= UNIQUE_VALUE_WARNING_THRESHOLD;

        // 如果有现有筛选，使用它的选中值；否则全选
        let selected_values = if let Some(filter) = current_filter {
            filter.selected_values.clone()
        } else {
            unique_values
                .iter()
                .map(|uv| uv.value.clone())
                .collect()
        };

        let filtered_unique_values = unique_values.clone();

        Self {
            col_ix,
            unique_values,
            selected_values,
            search_query: String::new(),
            filtered_unique_values,
            show_warning,
            is_truncated,
            total_unique_count: total_count,
        }
    }

    /// 获取列索引
    pub fn col_ix(&self) -> usize {
        self.col_ix
    }

    /// 获取唯一值列表
    pub fn unique_values(&self) -> &[UniqueValue] {
        &self.unique_values
    }

    /// 获取过滤后的唯一值列表
    pub fn filtered_unique_values(&self) -> &[UniqueValue] {
        &self.filtered_unique_values
    }

    /// 获取搜索查询
    pub fn search_query(&self) -> &str {
        &self.search_query
    }

    /// 更新搜索查询
    pub fn set_search_query(&mut self, query: String) {
        self.search_query = query;
        self.update_filtered_values();
    }

    /// 更新过滤后的唯一值列表
    fn update_filtered_values(&mut self) {
        if self.search_query.is_empty() {
            self.filtered_unique_values = self.unique_values.clone();
        } else {
            let query_lower = self.search_query.to_lowercase();
            self.filtered_unique_values = self
                .unique_values
                .iter()
                .filter(|uv| uv.value.to_lowercase().contains(&query_lower))
                .cloned()
                .collect();
        }
    }

    /// 切换值的选中状态
    pub fn toggle_value(&mut self, value: &str) {
        if self.selected_values.contains(value) {
            self.selected_values.remove(value);
        } else {
            self.selected_values.insert(value.to_string());
        }
    }

    /// 检查值是否被选中
    pub fn is_value_selected(&self, value: &str) -> bool {
        self.selected_values.contains(value)
    }

    /// 全选（选择所有可见的唯一值）
    pub fn select_all(&mut self) {
        for uv in &self.filtered_unique_values {
            self.selected_values.insert(uv.value.clone());
        }
    }

    /// 取消全选
    pub fn deselect_all(&mut self) {
        self.selected_values.clear();
    }

    /// 获取当前选中的值
    pub fn get_selected_values(&self) -> HashSet<String> {
        self.selected_values.clone()
    }

    /// 获取选中值的数量
    pub fn selected_count(&self) -> usize {
        self.selected_values.len()
    }

    /// 是否显示警告
    pub fn show_warning(&self) -> bool {
        self.show_warning
    }

    /// 是否被截断
    pub fn is_truncated(&self) -> bool {
        self.is_truncated
    }

    /// 获取总唯一值数量
    pub fn total_unique_count(&self) -> usize {
        self.total_unique_count
    }

    /// 获取警告消息
    pub fn warning_message(&self) -> Option<String> {
        if self.is_truncated {
            Some(format!(
                "⚠️ 此列包含 {} 个唯一值，仅显示前 {} 个。建议使用搜索功能缩小范围。",
                self.total_unique_count, UNIQUE_VALUE_HARD_LIMIT
            ))
        } else if self.show_warning {
            Some(format!(
                "⚠️ 此列包含 {} 个唯一值，加载可能较慢。建议使用搜索功能。",
                self.total_unique_count
            ))
        } else {
            None
        }
    }
}

// ============================================================================
// Unique Value Extraction
// ============================================================================

/// 唯一值及其计数
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueValue {
    pub value: String,
    pub count: usize,
}

impl UniqueValue {
    pub fn new(value: String, count: usize) -> Self {
        Self { value, count }
    }
}

/// 提取列的唯一值和计数
/// 
/// 使用 HashMap 一次遍历完成提取和计数，时间复杂度 O(n)
pub fn extract_unique_values(col_ix: usize, rows: &[Vec<String>]) -> Vec<UniqueValue> {
    let (values, _, _) = extract_unique_values_with_limit(col_ix, rows);
    values
}

/// 提取列的唯一值和计数（带限制）
/// 
/// 返回：(唯一值列表, 总数量, 是否被截断)
pub fn extract_unique_values_with_limit(
    col_ix: usize,
    rows: &[Vec<String>],
) -> (Vec<UniqueValue>, usize, bool) {
    let mut value_counts: HashMap<String, usize> = HashMap::new();

    // 一次遍历，统计每个值的出现次数
    for row in rows {
        let value = row
            .get(col_ix)
            .map(|s| s.clone())
            .unwrap_or_else(|| "NULL".to_string());

        *value_counts.entry(value).or_insert(0) += 1;
    }

    let total_count = value_counts.len();
    let is_truncated = total_count > UNIQUE_VALUE_HARD_LIMIT;

    // 转换为 Vec<UniqueValue>
    let mut unique_values: Vec<UniqueValue> = value_counts
        .into_iter()
        .map(|(value, count)| UniqueValue::new(value, count))
        .collect();

    // 按值排序（可选，提供更好的用户体验）
    unique_values.sort_by(|a, b| a.value.cmp(&b.value));

    // 如果超过硬限制，只保留前N个
    if is_truncated {
        unique_values.truncate(UNIQUE_VALUE_HARD_LIMIT);
    }

    (unique_values, total_count, is_truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_state_new() {
        let state = FilterState::new();
        assert!(state.filters.is_empty());
        assert!(state.filtered_row_indices.is_empty());
    }

    #[test]
    fn test_set_filter() {
        let mut state = FilterState::new();
        let mut selected = HashSet::new();
        selected.insert("value1".to_string());

        state.set_filter(0, selected.clone());

        assert!(state.is_column_filtered(0));
        assert_eq!(state.filters.get(&0).unwrap().selected_values, selected);
    }

    #[test]
    fn test_clear_filter() {
        let mut state = FilterState::new();
        let mut selected = HashSet::new();
        selected.insert("value1".to_string());

        state.set_filter(0, selected);
        assert!(state.is_column_filtered(0));

        state.clear_filter(0);
        assert!(!state.is_column_filtered(0));
    }

    #[test]
    fn test_clear_all() {
        let mut state = FilterState::new();
        let mut selected = HashSet::new();
        selected.insert("value1".to_string());

        state.set_filter(0, selected.clone());
        state.set_filter(1, selected);

        state.clear_all();

        assert!(!state.is_column_filtered(0));
        assert!(!state.is_column_filtered(1));
        assert!(state.filtered_row_indices.is_empty());
    }

    #[test]
    fn test_apply_filters_empty() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string(), "1".to_string()],
            vec!["b".to_string(), "2".to_string()],
        ];

        let indices = state.apply_filters(&rows);

        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn test_apply_filters_single_column() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string(), "1".to_string()],
            vec!["b".to_string(), "2".to_string()],
            vec!["a".to_string(), "3".to_string()],
        ];

        let mut selected = HashSet::new();
        selected.insert("a".to_string());
        state.set_filter(0, selected);

        let indices = state.apply_filters(&rows);

        assert_eq!(indices, vec![0, 2]);
    }

    #[test]
    fn test_apply_filters_multi_column_and_logic() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string(), "1".to_string()],
            vec!["b".to_string(), "1".to_string()],
            vec!["a".to_string(), "2".to_string()],
        ];

        let mut selected_col0 = HashSet::new();
        selected_col0.insert("a".to_string());
        state.set_filter(0, selected_col0);

        let mut selected_col1 = HashSet::new();
        selected_col1.insert("1".to_string());
        state.set_filter(1, selected_col1);

        let indices = state.apply_filters(&rows);

        // 只有第一行同时满足 col0="a" AND col1="1"
        assert_eq!(indices, vec![0]);
    }

    #[test]
    fn test_apply_filters_null_handling() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string()],
            vec!["NULL".to_string()],
        ];

        let mut selected = HashSet::new();
        selected.insert("NULL".to_string());
        state.set_filter(0, selected);

        let indices = state.apply_filters(&rows);

        assert_eq!(indices, vec![1]);
    }

    #[test]
    fn test_extract_unique_values_basic() {
        let rows = vec![
            vec!["a".to_string(), "1".to_string()],
            vec!["b".to_string(), "2".to_string()],
            vec!["a".to_string(), "3".to_string()],
        ];

        let unique = extract_unique_values(0, &rows);

        assert_eq!(unique.len(), 2);
        assert!(unique.contains(&UniqueValue::new("a".to_string(), 2)));
        assert!(unique.contains(&UniqueValue::new("b".to_string(), 1)));
    }

    #[test]
    fn test_extract_unique_values_with_null() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["NULL".to_string()],
            vec!["a".to_string()],
        ];

        let unique = extract_unique_values(0, &rows);

        assert_eq!(unique.len(), 2);
        assert!(unique.contains(&UniqueValue::new("a".to_string(), 2)));
        assert!(unique.contains(&UniqueValue::new("NULL".to_string(), 1)));
    }

    #[test]
    fn test_extract_unique_values_empty() {
        let rows: Vec<Vec<String>> = vec![];
        let unique = extract_unique_values(0, &rows);
        assert!(unique.is_empty());
    }

    #[test]
    fn test_extract_unique_values_single_row() {
        let rows = vec![vec!["a".to_string()]];
        let unique = extract_unique_values(0, &rows);

        assert_eq!(unique.len(), 1);
        assert_eq!(unique[0], UniqueValue::new("a".to_string(), 1));
    }

    // ============================================================================
    // Additional Edge Case Tests (Requirements 2.4)
    // ============================================================================

    #[test]
    fn test_set_filter_empty_selection() {
        let mut state = FilterState::new();
        let selected = HashSet::new();

        state.set_filter(0, selected);

        // 空选择集应该被视为未激活
        assert!(!state.is_column_filtered(0));
    }

    #[test]
    fn test_clear_filter_nonexistent_column() {
        let mut state = FilterState::new();

        // 清除不存在的列筛选不应该崩溃
        state.clear_filter(999);

        assert!(!state.is_column_filtered(999));
    }

    #[test]
    fn test_apply_filters_out_of_bounds_column() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string(), "1".to_string()],
            vec!["b".to_string(), "2".to_string()],
        ];

        // 设置超出范围的列索引
        let mut selected = HashSet::new();
        selected.insert("value".to_string());
        state.set_filter(999, selected);

        let indices = state.apply_filters(&rows);

        // 超出范围的列应该被忽略，返回空结果
        assert_eq!(indices, Vec::<usize>::new());
    }

    #[test]
    fn test_apply_filters_empty_rows() {
        let mut state = FilterState::new();
        let rows: Vec<Vec<String>> = vec![];

        let mut selected = HashSet::new();
        selected.insert("a".to_string());
        state.set_filter(0, selected);

        let indices = state.apply_filters(&rows);

        assert!(indices.is_empty());
    }

    #[test]
    fn test_apply_filters_single_row() {
        let mut state = FilterState::new();
        let rows = vec![vec!["a".to_string()]];

        let mut selected = HashSet::new();
        selected.insert("a".to_string());
        state.set_filter(0, selected);

        let indices = state.apply_filters(&rows);

        assert_eq!(indices, vec![0]);
    }

    #[test]
    fn test_apply_filters_no_match() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
        ];

        let mut selected = HashSet::new();
        selected.insert("c".to_string());
        state.set_filter(0, selected);

        let indices = state.apply_filters(&rows);

        assert!(indices.is_empty());
    }

    #[test]
    fn test_null_value_as_string() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string()],
            vec!["NULL".to_string()],
            vec!["b".to_string()],
        ];

        let mut selected = HashSet::new();
        selected.insert("NULL".to_string());
        state.set_filter(0, selected);

        let indices = state.apply_filters(&rows);

        assert_eq!(indices, vec![1]);
    }

    #[test]
    fn test_filtered_count() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["a".to_string()],
        ];

        let mut selected = HashSet::new();
        selected.insert("a".to_string());
        state.set_filter(0, selected);

        state.apply_filters(&rows);

        assert_eq!(state.filtered_count(), 2);
    }

    #[test]
    fn test_filtered_indices() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["a".to_string()],
        ];

        let mut selected = HashSet::new();
        selected.insert("a".to_string());
        state.set_filter(0, selected);

        state.apply_filters(&rows);

        assert_eq!(state.filtered_indices(), &[0, 2]);
    }

    #[test]
    fn test_extract_unique_values_out_of_bounds() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
        ];

        let unique = extract_unique_values(999, &rows);

        // 超出范围的列应该返回 NULL 值
        assert_eq!(unique.len(), 1);
        assert_eq!(unique[0].value, "NULL");
        assert_eq!(unique[0].count, 2);
    }

    #[test]
    fn test_multiple_filters_clear_one() {
        let mut state = FilterState::new();
        let rows = vec![
            vec!["a".to_string(), "1".to_string()],
            vec!["b".to_string(), "2".to_string()],
        ];

        let mut selected_col0 = HashSet::new();
        selected_col0.insert("a".to_string());
        state.set_filter(0, selected_col0);

        let mut selected_col1 = HashSet::new();
        selected_col1.insert("1".to_string());
        state.set_filter(1, selected_col1);

        // 清除第一列的筛选
        state.clear_filter(0);

        let indices = state.apply_filters(&rows);

        // 只有第二列的筛选生效
        assert_eq!(indices, vec![0]);
    }

    #[test]
    fn test_default_trait() {
        let state = FilterState::default();
        assert!(state.filters.is_empty());
        assert!(state.filtered_row_indices.is_empty());
    }

    // ============================================================================
    // FilterPanelState Unit Tests (Requirements 3.4, 2.5)
    // ============================================================================

    #[test]
    fn test_filter_panel_new_with_no_filter() {
        let rows = vec![
            vec!["a".to_string(), "1".to_string()],
            vec!["b".to_string(), "2".to_string()],
            vec!["a".to_string(), "3".to_string()],
        ];

        let panel = FilterPanelState::new(0, &rows, None);

        // 验证：默认应该全选
        assert_eq!(panel.selected_count(), 2); // "a" 和 "b"
        assert!(panel.is_value_selected("a"));
        assert!(panel.is_value_selected("b"));
        // 验证：小数据集不应该显示警告
        assert!(!panel.show_warning());
        assert!(!panel.is_truncated());
    }

    #[test]
    fn test_filter_panel_new_with_existing_filter() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
        ];

        let mut selected = HashSet::new();
        selected.insert("a".to_string());
        selected.insert("b".to_string());
        let existing_filter = ColumnFilter::new(selected.clone(), true);

        let panel = FilterPanelState::new(0, &rows, Some(&existing_filter));

        // 验证：应该使用现有筛选的选中值
        assert_eq!(panel.selected_count(), 2);
        assert!(panel.is_value_selected("a"));
        assert!(panel.is_value_selected("b"));
        assert!(!panel.is_value_selected("c"));
    }

    #[test]
    fn test_filter_panel_search_query() {
        let rows = vec![
            vec!["apple".to_string()],
            vec!["banana".to_string()],
            vec!["apricot".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 设置搜索查询
        panel.set_search_query("ap".to_string());

        // 验证：只有包含 "ap" 的值应该被过滤出来
        let filtered = panel.filtered_unique_values();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|uv| uv.value == "apple"));
        assert!(filtered.iter().any(|uv| uv.value == "apricot"));
        assert!(!filtered.iter().any(|uv| uv.value == "banana"));
    }

    #[test]
    fn test_filter_panel_search_case_insensitive() {
        let rows = vec![
            vec!["Apple".to_string()],
            vec!["BANANA".to_string()],
            vec!["apricot".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 使用小写搜索
        panel.set_search_query("ap".to_string());

        let filtered = panel.filtered_unique_values();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|uv| uv.value == "Apple"));
        assert!(filtered.iter().any(|uv| uv.value == "apricot"));
    }

    #[test]
    fn test_filter_panel_search_no_results() {
        let rows = vec![
            vec!["apple".to_string()],
            vec!["banana".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 搜索不存在的值
        panel.set_search_query("xyz".to_string());

        // 验证：应该返回空列表（Requirements 3.4）
        let filtered = panel.filtered_unique_values();
        assert!(filtered.is_empty(), "Search with no results should return empty list");
    }

    #[test]
    fn test_filter_panel_search_empty_query() {
        let rows = vec![
            vec!["apple".to_string()],
            vec!["banana".to_string()],
            vec!["apricot".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 先设置搜索查询
        panel.set_search_query("ap".to_string());
        assert_eq!(panel.filtered_unique_values().len(), 2);

        // 清除搜索查询
        panel.set_search_query(String::new());

        // 验证：应该恢复所有唯一值
        let filtered = panel.filtered_unique_values();
        assert_eq!(filtered.len(), 3);
    }

    #[test]
    fn test_filter_panel_toggle_value() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 初始状态：全选
        assert!(panel.is_value_selected("a"));

        // 切换
        panel.toggle_value("a");
        assert!(!panel.is_value_selected("a"));

        // 再次切换
        panel.toggle_value("a");
        assert!(panel.is_value_selected("a"));
    }

    #[test]
    fn test_filter_panel_select_all() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 先取消全选
        panel.deselect_all();
        assert_eq!(panel.selected_count(), 0);

        // 全选
        panel.select_all();
        assert_eq!(panel.selected_count(), 3);
        assert!(panel.is_value_selected("a"));
        assert!(panel.is_value_selected("b"));
        assert!(panel.is_value_selected("c"));
    }

    #[test]
    fn test_filter_panel_select_all_with_search() {
        let rows = vec![
            vec!["apple".to_string()],
            vec!["banana".to_string()],
            vec!["apricot".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 先取消全选
        panel.deselect_all();

        // 设置搜索查询
        panel.set_search_query("ap".to_string());

        // 全选（只选择可见的值）
        panel.select_all();

        // 验证：只有可见的值被选中
        assert!(panel.is_value_selected("apple"));
        assert!(panel.is_value_selected("apricot"));
        assert!(!panel.is_value_selected("banana"));
    }

    #[test]
    fn test_filter_panel_deselect_all() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 初始状态：全选
        assert!(panel.selected_count() > 0);

        // 取消全选
        panel.deselect_all();
        assert_eq!(panel.selected_count(), 0);
        assert!(!panel.is_value_selected("a"));
        assert!(!panel.is_value_selected("b"));
    }

    #[test]
    fn test_filter_panel_get_selected_values() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 取消全选
        panel.deselect_all();

        // 选择部分值
        panel.toggle_value("a");
        panel.toggle_value("c");

        let selected = panel.get_selected_values();
        assert_eq!(selected.len(), 2);
        assert!(selected.contains("a"));
        assert!(selected.contains("c"));
        assert!(!selected.contains("b"));
    }

    #[test]
    fn test_filter_panel_col_ix() {
        let rows = vec![vec!["a".to_string()]];
        let panel = FilterPanelState::new(5, &rows, None);

        assert_eq!(panel.col_ix(), 5);
    }

    #[test]
    fn test_filter_panel_unique_values() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["a".to_string()],
        ];

        let panel = FilterPanelState::new(0, &rows, None);

        let unique = panel.unique_values();
        assert_eq!(unique.len(), 2);
        assert!(unique.iter().any(|uv| uv.value == "a" && uv.count == 2));
        assert!(unique.iter().any(|uv| uv.value == "b" && uv.count == 1));
    }

    #[test]
    fn test_filter_panel_search_query_getter() {
        let rows = vec![vec!["a".to_string()]];
        let mut panel = FilterPanelState::new(0, &rows, None);

        assert_eq!(panel.search_query(), "");

        panel.set_search_query("test".to_string());
        assert_eq!(panel.search_query(), "test");
    }

    #[test]
    fn test_filter_panel_empty_data() {
        let rows: Vec<Vec<String>> = vec![];
        let panel = FilterPanelState::new(0, &rows, None);

        assert_eq!(panel.unique_values().len(), 0);
        assert_eq!(panel.filtered_unique_values().len(), 0);
        assert_eq!(panel.selected_count(), 0);
    }

    #[test]
    fn test_filter_panel_single_value() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["a".to_string()],
            vec!["a".to_string()],
        ];

        let panel = FilterPanelState::new(0, &rows, None);

        let unique = panel.unique_values();
        assert_eq!(unique.len(), 1);
        assert_eq!(unique[0].value, "a");
        assert_eq!(unique[0].count, 3);
    }

    #[test]
    fn test_filter_panel_null_values() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["NULL".to_string()],
            vec!["b".to_string()],
        ];

        let panel = FilterPanelState::new(0, &rows, None);

        let unique = panel.unique_values();
        assert_eq!(unique.len(), 3);
        assert!(unique.iter().any(|uv| uv.value == "NULL"));
    }

    #[test]
    fn test_filter_panel_search_preserves_selection() {
        let rows = vec![
            vec!["apple".to_string()],
            vec!["banana".to_string()],
            vec!["apricot".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 取消选择 banana
        panel.toggle_value("banana");
        let initial_selected = panel.get_selected_values();

        // 设置搜索查询
        panel.set_search_query("ap".to_string());

        // 验证：选中状态应该保持不变
        let current_selected = panel.get_selected_values();
        assert_eq!(current_selected, initial_selected);
        assert!(!panel.is_value_selected("banana"));
    }

    #[test]
    fn test_filter_panel_out_of_bounds_column() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
        ];

        // 列索引超出范围
        let panel = FilterPanelState::new(999, &rows, None);

        // 应该提取 NULL 值
        let unique = panel.unique_values();
        assert_eq!(unique.len(), 1);
        assert_eq!(unique[0].value, "NULL");
        assert_eq!(unique[0].count, 2);
    }

    #[test]
    fn test_filter_panel_special_characters() {
        let rows = vec![
            vec!["test@example.com".to_string()],
            vec!["user#123".to_string()],
            vec!["data$value".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 搜索包含特殊字符的值
        panel.set_search_query("@".to_string());

        let filtered = panel.filtered_unique_values();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].value, "test@example.com");
    }

    #[test]
    fn test_filter_panel_whitespace_values() {
        let rows = vec![
            vec!["  ".to_string()],
            vec!["a".to_string()],
            vec!["".to_string()],
        ];

        let panel = FilterPanelState::new(0, &rows, None);

        let unique = panel.unique_values();
        assert_eq!(unique.len(), 3);
        assert!(unique.iter().any(|uv| uv.value == "  "));
        assert!(unique.iter().any(|uv| uv.value == ""));
    }

    #[test]
    fn test_filter_panel_unicode_values() {
        let rows = vec![
            vec!["你好".to_string()],
            vec!["世界".to_string()],
            vec!["你好世界".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 搜索中文字符
        panel.set_search_query("你好".to_string());

        let filtered = panel.filtered_unique_values();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|uv| uv.value == "你好"));
        assert!(filtered.iter().any(|uv| uv.value == "你好世界"));
    }

    #[test]
    fn test_filter_panel_large_dataset() {
        // 生成大量数据
        let mut rows = Vec::new();
        for i in 0..1000 {
            rows.push(vec![format!("value_{}", i % 100)]);
        }

        let panel = FilterPanelState::new(0, &rows, None);

        // 验证：应该有 100 个唯一值
        let unique = panel.unique_values();
        assert_eq!(unique.len(), 100);

        // 验证：每个值应该出现 10 次
        for uv in unique {
            assert_eq!(uv.count, 10);
        }
    }

    #[test]
    fn test_filter_panel_search_substring_matching() {
        let rows = vec![
            vec!["prefix_middle_suffix".to_string()],
            vec!["another_middle_value".to_string()],
            vec!["no_match".to_string()],
        ];

        let mut panel = FilterPanelState::new(0, &rows, None);

        // 搜索中间的子串
        panel.set_search_query("middle".to_string());

        let filtered = panel.filtered_unique_values();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|uv| uv.value == "prefix_middle_suffix"));
        assert!(filtered.iter().any(|uv| uv.value == "another_middle_value"));
    }

    // ============================================================================
    // Performance Optimization Tests (Requirements 1.2, 5.5)
    // ============================================================================

    #[test]
    fn test_warning_threshold() {
        // 生成刚好达到警告阈值的数据
        let mut rows = Vec::new();
        for i in 0..UNIQUE_VALUE_WARNING_THRESHOLD {
            rows.push(vec![format!("value_{}", i)]);
        }

        let panel = FilterPanelState::new(0, &rows, None);

        // 验证：应该显示警告
        assert!(panel.show_warning());
        assert!(!panel.is_truncated());
        assert_eq!(panel.total_unique_count(), UNIQUE_VALUE_WARNING_THRESHOLD);
        assert!(panel.warning_message().is_some());
    }

    #[test]
    fn test_below_warning_threshold() {
        // 生成低于警告阈值的数据
        let mut rows = Vec::new();
        for i in 0..(UNIQUE_VALUE_WARNING_THRESHOLD - 1) {
            rows.push(vec![format!("value_{}", i)]);
        }

        let panel = FilterPanelState::new(0, &rows, None);

        // 验证：不应该显示警告
        assert!(!panel.show_warning());
        assert!(!panel.is_truncated());
        assert!(panel.warning_message().is_none());
    }

    #[test]
    fn test_hard_limit_truncation() {
        // 生成超过硬限制的数据
        let mut rows = Vec::new();
        for i in 0..(UNIQUE_VALUE_HARD_LIMIT + 100) {
            rows.push(vec![format!("value_{}", i)]);
        }

        let panel = FilterPanelState::new(0, &rows, None);

        // 验证：应该被截断
        assert!(panel.show_warning());
        assert!(panel.is_truncated());
        assert_eq!(panel.total_unique_count(), UNIQUE_VALUE_HARD_LIMIT + 100);
        assert_eq!(panel.unique_values().len(), UNIQUE_VALUE_HARD_LIMIT);
        
        let warning = panel.warning_message().unwrap();
        assert!(warning.contains("仅显示前"));
    }

    #[test]
    fn test_extract_unique_values_with_limit_no_truncation() {
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
        ];

        let (values, total, is_truncated) = extract_unique_values_with_limit(0, &rows);

        assert_eq!(values.len(), 3);
        assert_eq!(total, 3);
        assert!(!is_truncated);
    }

    #[test]
    fn test_extract_unique_values_with_limit_truncation() {
        // 生成超过硬限制的数据
        let mut rows = Vec::new();
        for i in 0..(UNIQUE_VALUE_HARD_LIMIT + 50) {
            rows.push(vec![format!("value_{}", i)]);
        }

        let (values, total, is_truncated) = extract_unique_values_with_limit(0, &rows);

        assert_eq!(values.len(), UNIQUE_VALUE_HARD_LIMIT);
        assert_eq!(total, UNIQUE_VALUE_HARD_LIMIT + 50);
        assert!(is_truncated);
    }

    #[test]
    fn test_warning_message_format() {
        // 测试警告消息格式
        let mut rows = Vec::new();
        for i in 0..UNIQUE_VALUE_WARNING_THRESHOLD {
            rows.push(vec![format!("value_{}", i)]);
        }

        let panel = FilterPanelState::new(0, &rows, None);
        let message = panel.warning_message().unwrap();

        assert!(message.contains("⚠️"));
        assert!(message.contains(&UNIQUE_VALUE_WARNING_THRESHOLD.to_string()));
    }

    #[test]
    fn test_truncation_message_format() {
        // 测试截断消息格式
        let mut rows = Vec::new();
        for i in 0..(UNIQUE_VALUE_HARD_LIMIT + 100) {
            rows.push(vec![format!("value_{}", i)]);
        }

        let panel = FilterPanelState::new(0, &rows, None);
        let message = panel.warning_message().unwrap();

        assert!(message.contains("⚠️"));
        assert!(message.contains("仅显示前"));
        assert!(message.contains(&UNIQUE_VALUE_HARD_LIMIT.to_string()));
    }

    #[test]
    fn test_performance_large_dataset() {
        // 性能测试：确保大数据集处理不会崩溃
        let mut rows = Vec::new();
        for i in 0..10000 {
            rows.push(vec![format!("value_{}", i % 100)]);
        }

        let start = std::time::Instant::now();
        let panel = FilterPanelState::new(0, &rows, None);
        let duration = start.elapsed();

        // 验证：应该在合理时间内完成（< 100ms）
        assert!(duration.as_millis() < 100, "Performance issue: took {:?}", duration);
        
        // 验证：数据正确
        assert_eq!(panel.unique_values().len(), 100);
        assert!(!panel.is_truncated());
    }

    #[test]
    fn test_search_performance_with_large_dataset() {
        // 性能测试：搜索功能在大数据集上的性能
        let mut rows = Vec::new();
        for i in 0..1000 {
            rows.push(vec![format!("value_{}", i)]);
        }

        let mut panel = FilterPanelState::new(0, &rows, None);

        let start = std::time::Instant::now();
        panel.set_search_query("value_5".to_string());
        let duration = start.elapsed();

        // 验证：搜索应该很快（< 10ms）
        assert!(duration.as_millis() < 10, "Search performance issue: took {:?}", duration);
        
        // 验证：搜索结果正确
        let filtered = panel.filtered_unique_values();
        assert!(filtered.len() > 0);
        assert!(filtered.iter().all(|uv| uv.value.contains("value_5")));
    }

    #[test]
    fn test_filter_application_performance() {
        // 性能测试：筛选应用的性能
        let mut rows = Vec::new();
        for i in 0..10000 {
            rows.push(vec![format!("value_{}", i % 100)]);
        }

        let mut state = FilterState::new();
        let mut selected = HashSet::new();
        selected.insert("value_50".to_string());
        state.set_filter(0, selected);

        let start = std::time::Instant::now();
        let indices = state.apply_filters(&rows);
        let duration = start.elapsed();

        // 验证：筛选应该很快（< 50ms）
        assert!(duration.as_millis() < 50, "Filter performance issue: took {:?}", duration);
        
        // 验证：结果正确
        assert_eq!(indices.len(), 100); // value_50 出现 100 次
    }
}
