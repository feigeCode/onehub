use gpui::SharedString;

/// 筛选行数据 - 表示列中的一个唯一值及其出现次数
#[derive(Clone, Debug)]
pub struct ColumnFilterValue {
    /// 值的字符串表示
    pub value: SharedString,
    /// 该值在列中出现的次数
    pub count: usize,
}

impl ColumnFilterValue {
    pub fn new(value: impl Into<SharedString>, count: usize) -> Self {
        Self {
            value: value.into(),
            count,
        }
    }
}
