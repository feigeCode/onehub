// 1. 标准库导入
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

// 2. 外部 crate 导入（按字母顺序）
use gpui::{App, AppContext, Context, Entity, IntoElement, InteractiveElement, ParentElement, Render, RenderOnce, Styled, Window, div, StatefulInteractiveElement, EventEmitter, SharedString, Focusable, FocusHandle, AsyncApp, px, prelude::FluentBuilder, Subscription, Task};
use gpui_component::{
    ActiveTheme, IconName, h_flex, list::ListItem,
    menu::{ContextMenuExt, PopupMenuItem},
    tree::TreeItem, v_flex, Icon, Sizable, Size,
    tooltip::Tooltip,
    button::{Button, ButtonVariants as _},
    input::{InputState, InputEvent, Input},
    spinner::Spinner,
    context_menu_tree::{context_menu_tree, ContextMenuTreeState},
    popover::Popover,
    checkbox::Checkbox,
    list::{List, ListDelegate, ListState},
    IndexPath, Selectable,
    clipboard::Clipboard,
};
use tracing::log::{error, info, trace};

// 3. 当前 crate 导入（按模块分组）
use db::{GlobalDbState, DbNode, DbNodeType};
use gpui_component::label::Label;
use crate::database_view_plugin::DatabaseViewPluginRegistry;
use one_core::{
    storage::{GlobalStorageState, StoredConnection},
};
use one_core::storage::DatabaseType;
use one_core::utils::debouncer::Debouncer;

// ============================================================================
// SQL 导出模式
// ============================================================================

/// SQL 导出模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDumpMode {
    /// 仅导出结构
    StructureOnly,
    /// 仅导出数据
    DataOnly,
    /// 导出结构和数据
    StructureAndData,
}

// ============================================================================
// DatabaseListItem - 数据库筛选列表项
// ============================================================================

#[derive(IntoElement)]
pub struct DatabaseListItem {
    db_id: String,
    db_name: String,
    is_selected: bool,
    selected: bool,
    view: Entity<DbTreeView>,
    connection_id: String,
}

impl DatabaseListItem {
    pub fn new(
        db_id: String,
        db_name: String,
        is_selected: bool,
        selected: bool,
        view: Entity<DbTreeView>,
        connection_id: String,
    ) -> Self {
        Self {
            db_id,
            db_name,
            is_selected,
            selected,
            view,
            connection_id,
        }
    }
}

impl Selectable for DatabaseListItem {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}

impl RenderOnce for DatabaseListItem {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let view_item = self.view.clone();
        let conn_item = self.connection_id.clone();
        let db_id_item = self.db_id.clone();
        let db_name_display = self.db_name.clone();
        let is_selected = self.is_selected;

        h_flex()
            .id(SharedString::from(format!("db-item-{}", self.db_id)))
            .w_full()
            .px_3()
            .py_2()
            .gap_2()
            .items_center()
            .cursor_pointer()
            .rounded(px(4.0))
            .when(self.selected, |el| {
                el.bg(cx.theme().list_active)
            })
            .when(!self.selected, |el| {
                el.hover(|style| style.bg(cx.theme().list_hover))
            })
            .on_click(move |_, _, cx| {
                view_item.update(cx, |this, cx| {
                    this.toggle_database_selection(&conn_item, &db_id_item, cx);
                });
            })
            .child(
                Checkbox::new(SharedString::from(format!("db-check-{}", self.db_id)))
                    .checked(is_selected)
            )
            .child(
                div()
                    .flex_1()
                    .text_sm()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(db_name_display)
            )
    }
}

// ============================================================================
// DatabaseListDelegate - 数据库筛选列表代理
// ============================================================================

pub struct DatabaseListDelegate {
    view: Entity<DbTreeView>,
    connection_id: String,
    databases: Vec<(String, String)>,
    filtered_databases: Vec<(String, String)>,
    selected_index: Option<IndexPath>,
}

impl DatabaseListDelegate {
    pub fn new(
        view: Entity<DbTreeView>,
        connection_id: String,
        databases: Vec<(String, String)>,
    ) -> Self {
        let filtered_databases = databases.clone();
        Self {
            view,
            connection_id,
            databases,
            filtered_databases,
            selected_index: None,
        }
    }
}

impl ListDelegate for DatabaseListDelegate {
    type Item = DatabaseListItem;

    fn perform_search(&mut self, query: &str, _window: &mut Window, cx: &mut Context<ListState<Self>>) -> Task<()> {
        if query.is_empty() {
            self.filtered_databases = self.databases.clone();
        } else {
            let query_lower = query.to_lowercase();
            self.filtered_databases = self.databases
                .iter()
                .filter(|(_, name)| name.to_lowercase().contains(&query_lower))
                .cloned()
                .collect();
        }
        cx.notify();
        Task::ready(())
    }

    fn items_count(&self, _section: usize, _cx: &App) -> usize {
        self.filtered_databases.len()
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let (db_id, db_name) = self.filtered_databases.get(ix.row)?.clone();
        let is_selected = self.view.read(cx).is_database_selected(&self.connection_id, &db_id);
        let selected = Some(ix) == self.selected_index;

        Some(DatabaseListItem::new(
            db_id,
            db_name,
            is_selected,
            selected,
            self.view.clone(),
            self.connection_id.clone(),
        ))
    }

    fn set_selected_index(
        &mut self,
        ix: Option<IndexPath>,
        _window: &mut Window,
        _cx: &mut Context<ListState<Self>>,
    ) {
        self.selected_index = ix;
    }
}

// ============================================================================
// DbTreeView Events
// ============================================================================

/// 数据库树视图事件
#[derive(Debug, Clone)]
pub enum DbTreeViewEvent {
    /// 打开表数据标签页
    OpenTableData { node_id: String },
    /// 打开视图数据标签页
    OpenViewData { node_id: String },
    /// 设计表（新建或编辑）
    DesignTable { node_id: String },
    /// 为指定数据库创建新查询
    CreateNewQuery { node_id: String },
    /// 打开命名查询
    OpenNamedQuery { node_id: String },
    /// 重命名查询
    RenameQuery { node_id: String },
    /// 删除查询
    DeleteQuery { node_id: String },
    /// 节点被选中（用于更新 objects panel）
    NodeSelected { node_id: String },
    /// 导入数据
    ImportData { node_id: String },
    /// 导出数据
    ExportData { node_id: String },
    /// 关闭连接
    CloseConnection { node_id: String },
    /// 删除连接
    DeleteConnection { node_id: String },
    /// 新建数据库
    CreateDatabase { node_id: String },
    /// 编辑数据库
    EditDatabase { node_id: String },
    /// 关闭数据库
    CloseDatabase { node_id: String },
    /// 删除数据库
    DeleteDatabase { node_id: String },
    /// 新建模式(Schema)
    CreateSchema { node_id: String },
    /// 删除模式(Schema)
    DeleteSchema { node_id: String },
    /// 删除表
    DeleteTable { node_id: String },
    /// 重命名表
    RenameTable { node_id: String },
    /// 清空表
    TruncateTable { node_id: String },
    /// 删除视图
    DeleteView { node_id: String },
    /// 运行SQL文件
    RunSqlFile { node_id: String },
    /// 转储SQL文件（导出结构和/或数据）
    DumpSqlFile { node_id: String, mode: SqlDumpMode },
}

/// 根据节点类型获取图标（公共函数，可被其他模块复用）
pub fn get_icon_for_node_type(node_type: &DbNodeType, _theme: &gpui_component::Theme) -> Icon {
    match node_type {
        DbNodeType::Connection => IconName::MySQLLineColor.color().with_size(Size::Large),
        DbNodeType::Schema => IconName::Schema.color(),
        DbNodeType::Database => Icon::from(IconName::Database).color().with_size(Size::Size(px(20.))),
        DbNodeType::Table => Icon::from(IconName::Table).color(),
        DbNodeType::View => Icon::from(IconName::View).color(),
        DbNodeType::Function => Icon::from(IconName::Function).color(),
        DbNodeType::Procedure => Icon::from(IconName::Procedure).color(),
        DbNodeType::Column => Icon::from(IconName::Column).color(),
        DbNodeType::Index => Icon::from(IconName::Index).color(),
        DbNodeType::Trigger => Icon::from(IconName::Trigger).color(),
        DbNodeType::Sequence => Icon::from(IconName::Sequence).color(),
        DbNodeType::NamedQuery => Icon::from(IconName::Query).color(),
        _ => IconName::File.color()
    }
}

// ============================================================================
// DbTreeView - 数据库连接树视图（支持懒加载）
// ============================================================================

pub struct DbTreeView {
    focus_handle: FocusHandle,
    tree_state: Entity<ContextMenuTreeState>,
    selected_item: Option<TreeItem>,
    // 存储 DbNode 映射 (ID -> DbNode)，用于懒加载
    db_nodes: HashMap<String, DbNode>,
    // 已经懒加载过子节点的集合
    loaded_children: HashSet<String>,
    // 正在加载的节点集合（用于显示加载状态）
    loading_nodes: HashSet<String>,
    // 加载失败的节点集合（用于显示错误状态）
    error_nodes: HashMap<String, String>,
    // 已展开的节点（用于在重建树时保持展开状态）
    expanded_nodes: HashSet<String>,
    // 当前连接名称或者工作区名称
    connection_name: Option<String>,
    // 工作区ID
    _workspace_id: Option<i64>,
    // 搜索输入框状态
    search_input: Entity<InputState>,
    // 搜索关键字
    search_query: String,
    // 搜索防抖序列号
    search_seq: u64,
    search_debouncer: Arc<Debouncer>,
    // 数据库筛选：连接ID -> 选中的数据库ID集合（None 表示全选）
    selected_databases: HashMap<String, Option<HashSet<String>>>,
    // 数据库筛选搜索词：连接ID -> 搜索词
    db_filter_search: HashMap<String, String>,
    // 数据库筛选列表状态：连接ID -> ListState
    db_filter_list_states: HashMap<String, Entity<ListState<DatabaseListDelegate>>>,

    _sub: Subscription
}

impl DbTreeView {
    /// 创建菜单项的辅助函数，避免重复克隆
    fn create_menu_item<F>(
        node_id: &str,
        label: String,
        view_clone: &Entity<Self>,
        window: &Window,
        event_creator: F,
    ) -> PopupMenuItem
    where
        F: Fn(String) -> DbTreeViewEvent + 'static,
    {
        let node_id = node_id.to_string();
        PopupMenuItem::new(label).on_click(window.listener_for(view_clone, move |_this, _, _, cx| {
            cx.emit(event_creator(node_id.clone()));
        }))
    }

    pub fn new(connections: &Vec<StoredConnection>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let mut db_nodes = HashMap::new();
        let mut init_nodes = vec![];
        let mut workspace_id = None;
        let mut unselected_databases_map = HashMap::new();

        if connections.is_empty() {
            let node =  DbNode::new("root", "No Database Connected", DbNodeType::Connection, "".to_string(), DatabaseType::MySQL);
            db_nodes.insert(
                "root".to_string(),
                node.clone()
            );
            init_nodes.push( node)
        }else {
            for conn in connections {
                workspace_id = conn.workspace_id;
                let id = conn.id.unwrap_or(0).to_string();

                let conn_config = match conn.to_db_connection() {
                    Ok(config) => config,
                    Err(e) => {
                        tracing::error!("无法解析连接配置 {}: {}", id, e);
                        continue;
                    }
                };

                // 读取已选中的数据库列表
                if let Some(selected_dbs) = conn.get_selected_databases() {
                    let selected: HashSet<String> = selected_dbs.into_iter().collect();
                    unselected_databases_map.insert(id.clone(), Some(selected));
                }

                let node = DbNode::new(id.clone(), conn_config.name.to_string(), DbNodeType::Connection, id.clone(), conn_config.database_type);
                db_nodes.insert(id, node.clone());
                init_nodes.push(node);
            }
        }
        init_nodes.sort();
        let items = Self::create_initial_tree(init_nodes);
        let tree_state = cx.new(|cx| {
            ContextMenuTreeState::new(cx).items(items)
        });
        let search_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder("搜索...").clean_on_escape()
        });
        let search_debouncer = Arc::new(Debouncer::new(Duration::from_millis(250)));

        let _sub = cx.subscribe_in(&search_input, window, |this: &mut Self, input: &Entity<InputState>, event: &InputEvent, _window, cx: &mut Context<Self>| {
            if let InputEvent::Change = event {
                let query = input.read(cx).text().to_string();

                this.search_seq += 1;
                let current_seq = this.search_seq;
                let debouncer = Arc::clone(&this.search_debouncer);
                let query_for_task = query.clone();

                cx.spawn(async move |view, cx| {
                    if debouncer.debounce().await {
                        _ = view.update(cx, |this, cx| {
                            if this.search_seq == current_seq {
                                this.search_query = query_for_task.clone();
                                this.rebuild_tree(cx);
                            }
                        });
                    }
                }).detach();
            }
        });

        Self {
            focus_handle,
            tree_state,
            selected_item: None,
            db_nodes,
            loaded_children: HashSet::new(),
            loading_nodes: HashSet::new(),
            error_nodes: HashMap::new(),
            expanded_nodes: HashSet::new(),
            connection_name: None,
            _workspace_id: workspace_id,
            search_input,
            search_query: String::new(),
            search_seq: 0,
            search_debouncer,
            selected_databases: unselected_databases_map,
            db_filter_search: HashMap::new(),
            db_filter_list_states: HashMap::new(),
            _sub
        }
    }

    /// 折叠所有节点
    pub fn collapse_all(&mut self, cx: &mut Context<Self>) {
        self.expanded_nodes.clear();
        self.rebuild_tree(cx);
    }

    /// 创建初始树结构（未连接状态）
    fn create_initial_tree(init_nodes: Vec<DbNode>) -> Vec<TreeItem> {
        if init_nodes.is_empty() {
            return vec![
                TreeItem::new("root".to_string(), "No Database Connected".to_string())
            ]
        }
        let mut items: Vec<TreeItem> = Vec::new();
        for node in init_nodes.iter() {
            items.push(TreeItem::new(SharedString::new(node.id.to_string()), SharedString::new(node.name.to_string())))
        }
        items
    }

    /// 设置连接名称
    pub fn set_connection_name(&mut self, name: String) {
        self.connection_name = Some(name);
    }

    /// 获取连接下的所有数据库节点
    pub fn get_databases_for_connection(&self, connection_id: &str) -> Vec<(String, String)> {
        let mut databases = Vec::new();
        if let Some(conn_node) = self.db_nodes.get(connection_id) {
            for child in &conn_node.children {
                if child.node_type == DbNodeType::Database {
                    databases.push((child.id.clone(), child.name.clone()));
                }
            }
        }
        databases
    }

    /// 获取选中的数据库数量
    pub fn get_selected_database_count(&self, connection_id: &str) -> (usize, usize) {
        let databases = self.get_databases_for_connection(connection_id);
        let total = databases.len();

        match self.selected_databases.get(connection_id) {
            None => (total, total),
            Some(None) => (total, total),
            Some(Some(selected)) => {
                let count = databases.iter().filter(|(id, _)| selected.contains(id)).count();
                (count, total)
            }
        }
    }

    /// 切换数据库选中状态
    pub fn toggle_database_selection(&mut self, connection_id: &str, database_id: &str, cx: &mut Context<Self>) {
        let databases = self.get_databases_for_connection(connection_id);
        let all_db_ids: HashSet<String> = databases.iter().map(|(id, _)| id.clone()).collect();

        let selected = self.selected_databases
            .entry(connection_id.to_string())
            .or_insert(None);

        match selected {
            None => {
                let mut new_selected = all_db_ids.clone();
                new_selected.remove(database_id);
                *selected = Some(new_selected);
            }
            Some(set) => {
                if set.contains(database_id) {
                    set.remove(database_id);
                } else {
                    set.insert(database_id.to_string());
                }
                if set.len() == all_db_ids.len() {
                    *selected = None;
                }
            }
        }

        self.rebuild_tree(cx);
        self.save_database_filter(connection_id, cx);
    }

    /// 全选数据库
    pub fn select_all_databases(&mut self, connection_id: &str, cx: &mut Context<Self>) {
        self.selected_databases.insert(connection_id.to_string(), None);
        self.rebuild_tree(cx);
        self.save_database_filter(connection_id, cx);
    }

    /// 清除筛选（取消全选）
    pub fn deselect_all_databases(&mut self, connection_id: &str, cx: &mut Context<Self>) {
        self.selected_databases.insert(connection_id.to_string(), Some(HashSet::new()));
        self.rebuild_tree(cx);
        self.save_database_filter(connection_id, cx);
    }

    /// 保存数据库筛选状态到存储
    fn save_database_filter(&self, connection_id: &str, cx: &mut Context<Self>) {
        let selected_dbs: Option<Vec<String>> = match self.selected_databases.get(connection_id) {
            None => None, // 全选
            Some(None) => None, // 全选
            Some(Some(selected_set)) => {
                // 保存已选中的数据库列表
                Some(selected_set.iter().cloned().collect())
            }
        };

        let connection_id_str = connection_id.to_string();
        let storage = cx.global::<GlobalStorageState>().storage.clone();

        cx.spawn(async move |_view, cx| {
            use one_core::storage::traits::Repository;
            use one_core::storage::ConnectionRepository;
            use one_core::gpui_tokio::Tokio;

            let conn_id: i64 = match connection_id_str.parse() {
                Ok(id) => id,
                Err(_) => return Ok::<(), anyhow::Error>(()),
            };

            let result = Tokio::spawn_result(cx, async move {
                if let Some(repo_arc) = storage.get::<ConnectionRepository>().await {
                    let repo = (*repo_arc).clone();
                    if let Ok(Some(mut conn)) = repo.get(conn_id).await {
                        conn.set_selected_databases(selected_dbs);
                        let _ = repo.update(&mut conn).await;
                    }
                }
                Ok(())
            })?.await;

            let _ = result;
            Ok(())
        }).detach();
    }

    /// 检查数据库是否被选中
    pub fn is_database_selected(&self, connection_id: &str, database_id: &str) -> bool {
        match self.selected_databases.get(connection_id) {
            None => true,
            Some(None) => true,
            Some(Some(set)) => set.contains(database_id),
        }
    }

    /// 检查是否全选
    pub fn is_all_selected(&self, connection_id: &str) -> bool {
        match self.selected_databases.get(connection_id) {
            None => true,
            Some(None) => true,
            Some(Some(set)) => {
                let databases = self.get_databases_for_connection(connection_id);
                set.len() == databases.len()
            }
        }
    }

    /// 设置数据库筛选搜索词
    pub fn set_db_filter_search(&mut self, connection_id: &str, query: String, cx: &mut Context<Self>) {
        self.db_filter_search.insert(connection_id.to_string(), query);
        cx.notify();
    }

    /// 获取数据库筛选搜索词
    pub fn get_db_filter_search(&self, connection_id: &str) -> String {
        self.db_filter_search.get(connection_id).cloned().unwrap_or_default()
    }

    /// 获取过滤后的数据库列表
    pub fn get_filtered_databases(&self, connection_id: &str) -> Vec<(String, String)> {
        let databases = self.get_databases_for_connection(connection_id);
        let search_query = self.get_db_filter_search(connection_id).to_lowercase();

        if search_query.is_empty() {
            databases
        } else {
            databases
                .into_iter()
                .filter(|(_, name)| name.to_lowercase().contains(&search_query))
                .collect()
        }
    }


    /// 刷新指定节点及其子节点
    /// 
    /// 这个方法会：
    /// 1. 清除节点的子节点缓存
    /// 2. 递归清除所有后代节点
    /// 3. 重新加载子节点
    /// 4. 如果节点已展开，保持展开状态
    pub fn refresh_tree(&mut self, node_id: String, cx: &mut Context<Self>) {
        info!("Refreshing node in DbTreeView: {}", node_id);
        
        // 递归清除节点及其所有后代
        self.clear_node_descendants(&node_id);
        
        // 清除加载状态和错误状态
        self.loaded_children.remove(&node_id);
        self.loading_nodes.remove(&node_id);
        self.error_nodes.remove(&node_id);
        
        // 重置节点状态
        if let Some(node) = self.db_nodes.get_mut(&node_id) {
            node.children_loaded = false;
            node.children.clear();
        }
        
        // 如果节点已展开，重新加载子节点
        if self.expanded_nodes.contains(&node_id) {
            self.lazy_load_children(node_id, cx);
        } else {
            // 如果节点未展开，只需重建树以更新占位符
            self.rebuild_tree(cx);
        }
    }
    
    /// 递归清除节点的所有后代
    fn clear_node_descendants(&mut self, node_id: &str) {
        // 获取当前节点的所有子节点ID
        let child_ids: Vec<String> = if let Some(node) = self.db_nodes.get(node_id) {
            node.children.iter().map(|c| c.id.clone()).collect()
        } else {
            return;
        };
        
        // 递归清除每个子节点
        for child_id in child_ids {
            self.clear_node_descendants(&child_id);
            
            // 从所有集合中移除子节点
            self.db_nodes.remove(&child_id);
            self.loaded_children.remove(&child_id);
            self.loading_nodes.remove(&child_id);
            self.error_nodes.remove(&child_id);
            self.expanded_nodes.remove(&child_id);
        }
    }

    /// 懒加载节点的子节点
    fn lazy_load_children(&mut self, node_id: String, cx: &mut Context<Self>) {
        // 如果已经加载过或正在加载，跳过
        if self.loaded_children.contains(&node_id) || self.loading_nodes.contains(&node_id) {
            return;
        }

        // 获取节点信息
        let node = match self.db_nodes.get(&node_id) {
            Some(n) => n.clone(),
            None => {
                error!("DbTreeView lazy_load_children: node not found in db_nodes: {}", node_id);
                return;
            }
        };

        info!("DbTreeView lazy_load_children: attempting to load children for: {} (type: {:?})",
              node_id, node.node_type);

        // 标记为正在加载
        self.loading_nodes.insert(node_id.clone());
        cx.notify();

        let global_state = cx.global::<GlobalDbState>().clone();
        let global_storage_state = cx.global::<GlobalStorageState>().clone();
        let clone_node_id = node_id.clone();
        let connection_id = node.connection_id.clone();
        
        cx.spawn(async move |this, cx: &mut AsyncApp| {
            // 使用 DatabasePlugin 的方法加载子节点，添加超时机制
            let children_result = global_state.load_node_children(cx, connection_id.clone(), node.clone(), global_storage_state.clone()).await;

            this.update(cx, |this: &mut Self, cx| {
                // 移除加载状态
                this.loading_nodes.remove(&clone_node_id);

                match children_result {
                    Ok(children) => {
                        info!("DbTreeView lazy_load_children: loaded {} children for node: {}", children.len(), clone_node_id);
                        // 标记为已加载，清除错误状态
                        this.loaded_children.insert(clone_node_id.clone());
                        this.error_nodes.remove(&clone_node_id);

                        // 更新节点的子节点
                        if let Some(parent_node) = this.db_nodes.get_mut(&clone_node_id) {
                            parent_node.children = children.clone();
                            parent_node.children_loaded = true;
                        }

                        // 递归地将所有子节点及其后代添加到 db_nodes
                        fn insert_nodes_recursive(
                            db_nodes: &mut HashMap<String, DbNode>,
                            node: &DbNode,
                        ) {
                            db_nodes.insert(node.id.clone(), node.clone());
                            for child in &node.children {
                                insert_nodes_recursive(db_nodes, child);
                            }
                        }

                        for child in &children {
                            trace!("DbTreeView lazy_load_children: adding child: {} (type: {:?})", child.id, child.node_type);
                            insert_nodes_recursive(&mut this.db_nodes, child);
                        }

                        // 重建树结构
                        this.rebuild_tree(cx);
                    }
                    Err(e) => {
                        error!("DbTreeView lazy_load_children: failed to execute load_node_children for {}: {}", clone_node_id, e);
                        this.expanded_nodes.remove(&clone_node_id);
                        // 记录错误状态
                        this.error_nodes.insert(clone_node_id.clone(), e.to_string());
                        this.rebuild_tree(cx);
                    }
                }
            }).ok();
        }).detach();
    }

    /// 重建整个树结构（保留连接列表）
    pub fn rebuild_tree(&mut self, cx: &mut Context<Self>) {
        // 从真正的根节点重建（不依赖 self.items，因为它可能过时）
        // 找到所有顶层节点（在 db_nodes 中但不是任何节点的子节点）
        let mut root_nodes: Vec<DbNode> = Vec::new();

        for node in self.db_nodes.values() {
            if node.parent_context == None {
                root_nodes.push(node.clone());
            }
        }

        // 如果没有根节点，保留当前的树
        if root_nodes.is_empty() {
            return;
        }
        // 排序
        root_nodes.sort();

        let search_query = self.search_query.to_lowercase();

        // 使用找到的根节点ID构建树
        let root_items: Vec<TreeItem> = root_nodes
            .iter()
            .filter_map(|node| {
                Self::db_node_to_tree_item_filtered(
                    node,
                    &self.db_nodes,
                    &self.expanded_nodes,
                    &search_query,
                    &self.selected_databases,
                    None,
                )
            })
            .collect();
        // 只有当有新的items时才更新
        if !root_items.is_empty() || !search_query.is_empty() {
            self.tree_state.update(cx, |state, cx| {
                state.set_items(root_items, cx);
            });
        }
    }
    /// 递归构建过滤后的 TreeItem
    /// 已加载的节点：如果有匹配的子节点则自动展开
    /// 未加载的节点：不搜索、不展开
    fn db_node_to_tree_item_filtered(
        node: &DbNode,
        db_nodes: &HashMap<String, DbNode>,
        expanded_nodes: &HashSet<String>,
        query: &str,
        selected_databases: &HashMap<String, Option<HashSet<String>>>,
        current_connection_id: Option<&str>,
    ) -> Option<TreeItem> {
        // 确定当前所属的连接ID
        let conn_id = if node.node_type == DbNodeType::Connection {
            Some(node.id.as_str())
        } else {
            current_connection_id
        };

        // 如果是数据库节点，检查是否被选中
        if node.node_type == DbNodeType::Database {
            if let Some(conn_id) = conn_id {
                let is_selected = match selected_databases.get(conn_id) {
                    None => true,
                    Some(None) => true,
                    Some(Some(set)) => set.contains(&node.id),
                };
                if !is_selected {
                    return None;
                }
            }
        }

        // 检查当前节点是否匹配
        let self_matches = query.is_empty() || node.name.to_lowercase().contains(query);

        let mut item = TreeItem::new(node.id.clone(), node.name.clone());

        // 处理子节点
        let mut has_matching_children = false;
        let mut should_expand = false;

        if node.children_loaded && !node.children.is_empty() {
            // 已加载的节点：递归搜索子节点
            let children: Vec<TreeItem> = node
                .children
                .iter()
                .filter_map(|child_node| {
                    let child = if let Some(updated) = db_nodes.get::<str>(child_node.id.as_ref()) {
                        updated
                    } else {
                        child_node
                    };
                    Self::db_node_to_tree_item_filtered(
                        child,
                        db_nodes,
                        expanded_nodes,
                        query,
                        selected_databases,
                        conn_id,
                    )
                })
                .collect();

            if !children.is_empty() {
                has_matching_children = true;
                item = item.children(children);
                // 如果有搜索关键字且有匹配的子节点，自动展开
                should_expand = !query.is_empty();
            }
        } else if !node.children_loaded && query.is_empty() {
            // 未加载子节点但可能有子节点的节点：添加空占位符以显示展开箭头
            // 需要占位符的节点类型：
            // - Connection: 下有数据库
            // - Database: 下有 Schema 或 Tables/Views 文件夹
            // - Schema: 下有 Tables/Views 等文件夹
            // - Table: 下有 Columns、Indexes
            // - 各种文件夹类型: 下有具体对象
            let needs_placeholder = matches!(
                node.node_type,
                DbNodeType::Table
                | DbNodeType::TablesFolder
                | DbNodeType::ViewsFolder
                | DbNodeType::ColumnsFolder
                | DbNodeType::IndexesFolder
                | DbNodeType::FunctionsFolder
                | DbNodeType::ProceduresFolder
                | DbNodeType::TriggersFolder
                | DbNodeType::SequencesFolder
                | DbNodeType::QueriesFolder
                | DbNodeType::ForeignKeysFolder
                | DbNodeType::ChecksFolder
            );

            if needs_placeholder {
                let placeholder = TreeItem::new(
                    format!("{}:placeholder", node.id),
                    "loading..."
                );
                item = item.children(vec![placeholder]);
            }
        }

        // 设置展开状态
        if should_expand || (query.is_empty() && expanded_nodes.contains(&node.id)) {
            item = item.expanded(true);
        }

        // 如果当前节点匹配或有匹配的子节点，则显示
        if self_matches || has_matching_children {
            Some(item)
        } else {
            None
        }
    }

    fn render_database_filter_popover(
        view: &Entity<Self>,
        connection_id: &str,
        list_state: &Entity<ListState<DatabaseListDelegate>>,
        cx: &mut App,
    ) -> gpui::AnyElement {
        let view_clone = view.clone();
        let conn_id = connection_id.to_string();
        let is_all_selected = view.read(cx).is_all_selected(&conn_id);

        v_flex()
            .w(px(280.0))
            .max_h(px(400.0))
            .gap_2()
            .p_2()
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .px_1()
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child({
                                let view_select = view_clone.clone();
                                let conn_select = conn_id.clone();
                                Checkbox::new("select-all")
                                    .checked(is_all_selected)
                                    .on_click(move |_, _, cx| {
                                        view_select.update(cx, |this, cx| {
                                            if this.is_all_selected(&conn_select) {
                                                this.deselect_all_databases(&conn_select, cx);
                                            } else {
                                                this.select_all_databases(&conn_select, cx);
                                            }
                                        });
                                    })
                            })
                            .child(
                                div()
                                    .text_sm()
                                    .child("全选")
                            )
                    )
                    .child({
                        let view_clear = view_clone.clone();
                        let conn_clear = conn_id.clone();
                        Button::new("clear-filter")
                            .ghost()
                            .small()
                            .label("清除筛选")
                            .on_click(move |_, _, cx| {
                                view_clear.update(cx, |this, cx| {
                                    this.deselect_all_databases(&conn_clear, cx);
                                });
                            })
                    })
            )
            .child(
                div()
                    .border_t_1()
                    .border_color(cx.theme().border)
            )
            .child(
                List::new(list_state)
                    .w_full()
                    .max_h(px(320.0))
                    .p(px(8.))
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded(cx.theme().radius)
            )
            .into_any_element()
    }

    /// 根据节点类型获取图标
    fn get_icon_for_node(&self, node_id: &str, _is_expanded: bool, _cx: &mut Context<Self>) -> Icon {
        let node = self.db_nodes.get(node_id);
        match node.map(|n| &n.node_type) {
            Some(DbNodeType::Connection) => {
                if let Some(n) = node {
                    n.database_type.as_node_icon()
                } else {
                    IconName::Database.color().with_size(Size::Large)
                }
            }
            Some(DbNodeType::Database) => Icon::from(IconName::Database).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::Schema) => Icon::from(IconName::Schema).color().with_size(Size::Size(px(20.))),

            Some(DbNodeType::TablesFolder) => Icon::from(IconName::FolderTables).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::ViewsFolder) => Icon::from(IconName::FolderViews).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::FunctionsFolder) => Icon::from(IconName::FolderFunctions).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::ProceduresFolder) => Icon::from(IconName::FolderProcedures).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::TriggersFolder) => Icon::from(IconName::FolderTriggers).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::ForeignKeysFolder) => Icon::from(IconName::FolderForeignKeys).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::ChecksFolder) => Icon::from(IconName::FolderCheckConstraints).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::QueriesFolder) => Icon::from(IconName::FolderQueries).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::ColumnsFolder) => Icon::from(IconName::FolderColumns).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::IndexesFolder) => Icon::from(IconName::FolderIndexes).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::SequencesFolder) => Icon::from(IconName::FolderSequences).color().with_size(Size::Size(px(20.))),

            Some(DbNodeType::Table) => Icon::from(IconName::Table).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::View) => Icon::from(IconName::View).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::Function) => Icon::from(IconName::Function).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::Procedure) => Icon::from(IconName::Procedure).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::Column) => {
                let is_primary_key = node
                    .and_then(|n| n.metadata.as_ref())
                    .and_then(|m| m.get("is_primary_key"))
                    .map(|v| v == "true")
                    .unwrap_or(false);
                if is_primary_key {
                    Icon::from(IconName::PrimaryKey).color().with_size(Size::Size(px(20.)))
                } else {
                    Icon::from(IconName::Column).color().with_size(Size::Size(px(20.)))
                }
            }
            Some(DbNodeType::Index) => Icon::from(IconName::Index).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::ForeignKey) => Icon::from(IconName::GoldKey).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::Trigger) => Icon::from(IconName::Trigger).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::Sequence) => Icon::from(IconName::Sequence).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::Check) => Icon::from(IconName::CheckConstraint).color().with_size(Size::Size(px(20.))),
            Some(DbNodeType::NamedQuery) => Icon::from(IconName::Query).color().with_size(Size::Size(px(20.))),
            _ => Icon::from(IconName::Loader).with_size(Size::Size(px(14.))),
        }
    }

    pub fn active_connection(&mut self, active_conn_id: String,  cx: &mut Context<Self>) {
        self.expanded_nodes.insert(active_conn_id.clone());
        self.lazy_load_children(active_conn_id, cx);
        self.rebuild_tree(cx);
    }

    fn handle_item_double_click(&mut self, item: TreeItem, cx: &mut Context<Self>) {
        let node_id = item.id.to_string();
        
        // 如果节点有错误，双击重试连接
        if self.error_nodes.contains_key(&node_id) {
            self.error_nodes.remove(&node_id);
            self.lazy_load_children(node_id, cx);
            return;
        }
        
        // 根据节点类型执行不同的操作
        if let Some(node) = self.db_nodes.get(item.id.as_ref()).cloned() {
            match node.node_type {
                DbNodeType::Table => {
                    // 查找所属数据库
                    if let Some(database) = self.find_parent_database(&node.id) {
                        info!("DbTreeView: opening table data tab: {}.{}", database, node.name);
                        cx.emit(DbTreeViewEvent::OpenTableData {
                            node_id: node.id.clone()
                        });
                    }
                }
                DbNodeType::View => {
                    // 查找所属数据库
                    if let Some(database) = self.find_parent_database(&node.id) {
                        info!("DbTreeView: opening view data tab: {}.{}", database, node.name);
                        cx.emit(DbTreeViewEvent::OpenViewData {
                            node_id: node.id.clone()
                        });
                    }
                }
                DbNodeType::NamedQuery => {
                    // 打开命名查询
                    info!("DbTreeView: opening named query: {}", node.name);
                    cx.emit(DbTreeViewEvent::OpenNamedQuery {
                        node_id: node.id.clone()
                    });
                }
                DbNodeType::Connection | DbNodeType::Database | DbNodeType::Schema |
                DbNodeType::ColumnsFolder | DbNodeType::IndexesFolder |
                DbNodeType::FunctionsFolder | DbNodeType::ProceduresFolder |
                DbNodeType::TriggersFolder | DbNodeType::QueriesFolder |
                DbNodeType::TablesFolder | DbNodeType::ViewsFolder  => {
                    let node_id = item.id.to_string();
                    let is_expanded = self.expanded_nodes.contains(&node_id);
                    
                    // 切换展开状态
                    if is_expanded {
                        self.expanded_nodes.remove(&node_id);
                    } else {
                        self.expanded_nodes.insert(node_id.clone());
                    }
                    
                    // 如果是展开操作，加载子节点（如果尚未加载）
                    if !is_expanded {
                        self.lazy_load_children(node_id, cx);
                    }
                    // 无论展开还是折叠，都需要重建树以更新展开状态
                    self.rebuild_tree(cx);
                }
                _ => {
                    // 其他类型的节点暂不处理双击
                }
            }
            cx.emit(DbTreeViewEvent::NodeSelected {
                node_id: node_id.clone()
            })
        }
        cx.notify();
    }

    
    fn handle_item_click(&mut self, item: TreeItem, cx: &mut Context<Self>) {
        self.selected_item = Some(item.clone());
        if self.db_nodes.contains_key(item.id.as_ref()) {
            // 发出节点选择事件
            cx.emit(DbTreeViewEvent::NodeSelected {
                node_id: item.id.to_string()
            });
            cx.notify();
        }
    }

    /// 获取节点信息（公开方法）
    pub fn get_node(&self, node_id: &str) -> Option<&DbNode> {
        self.db_nodes.get(node_id)
    }

    /// 关闭连接并清理相关状态
    pub fn close_connection(&mut self, connection_id: &str, cx: &mut Context<Self>) {
        info!("Closing connection in DbTreeView: {}", connection_id);
        
        // 清理连接节点的所有后代
        self.clear_node_descendants(connection_id);
        
        // 将连接节点重置为未连接状态
        if let Some(node) = self.db_nodes.get_mut(connection_id) {
            node.children.clear();
            node.children_loaded = false;
        }
        
        // 确保节点处于收起状态
        self.expanded_nodes.remove(connection_id);
        
        // 清理加载和错误状态
        self.loaded_children.remove(connection_id);
        self.loading_nodes.remove(connection_id);
        self.error_nodes.remove(connection_id);
        
        // 重建树以反映变化
        self.rebuild_tree(cx);
    }

    /// 关闭数据库并清理相关状态
    pub fn close_database(&mut self, database_node_id: &str, cx: &mut Context<Self>) {
        info!("Closing database in DbTreeView: {}", database_node_id);
        
        // 清理数据库节点的所有后代
        self.clear_node_descendants(database_node_id);
        
        // 将数据库节点重置为未展开状态
        if let Some(node) = self.db_nodes.get_mut(database_node_id) {
            node.children.clear();
            node.children_loaded = false;
        }
        
        // 确保节点处于收起状态
        self.expanded_nodes.remove(database_node_id);
        
        // 清理加载和错误状态
        self.loaded_children.remove(database_node_id);
        self.loading_nodes.remove(database_node_id);
        self.error_nodes.remove(database_node_id);
        
        // 重建树以反映变化
        self.rebuild_tree(cx);
    }

    /// 获取当前选中的数据库名称
    pub fn get_selected_database(&self) -> Option<String> {
        if let Some(item) = &self.selected_item {
            // 从选中的节点ID中提取数据库名
            if let Some(node) = self.db_nodes.get(item.id.as_ref()) {
                return match node.node_type {
                    DbNodeType::Database => {
                        Some(node.name.clone())
                    }
                    _ => {
                        // 从父节点上下文中查找数据库
                        self.find_parent_database(item.id.as_ref())
                    }
                }
            }
        }
        None
    }

    /// 查找节点所属的数据库名称
    fn find_parent_database(&self, node_id: &str) -> Option<String> {
        // 向上遍历查找数据库节点
        let mut current_id = node_id.to_string();

        while let Some(node) = self.db_nodes.get(&current_id) {
            if node.node_type == DbNodeType::Database {
                return Some(node.name.clone());
            }

            // 查找父节点
            let parent_found = self.db_nodes.values().find(|parent| {
                parent.children.iter().any(|child| child.id == current_id)
            });

            if let Some(parent) = parent_found {
                current_id = parent.id.clone();
            } else {
                break;
            }
        }

        None
    }
}

impl Render for DbTreeView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let view = cx.entity();

        v_flex()
            .id("db-tree-view")
            .size_full()
            .bg(cx.theme().sidebar)
            .child({
                let view_for_collapse = cx.entity();
                h_flex()
                    .w_full()
                    .p_1()
                    .gap_1()
                    .border_t_1()
                    .border_color(cx.theme().sidebar_border)
                    .bg(cx.theme().sidebar)
                    .child(
                        div()
                            .flex_1()
                            .child(Input::new(&self.search_input)
                                .prefix(
                                    Icon::new(IconName::Search)
                                        .text_color(cx.theme().muted_foreground),
                                )
                                .cleanable(true)
                                .small()
                                .w_full())
                    )
                    .child(
                        Button::new("collapse-all")
                            .icon(IconName::ChevronsUpDown)
                            .ghost()
                            .small()
                            .tooltip("折叠所有")
                            .on_click(move |_, _, cx| {
                                view_for_collapse.update(cx, |this, cx| {
                                    this.collapse_all(cx);
                                });
                            })
                    )
            })
            .child(
                // 树形视图
                v_flex()
                    .flex_1()
                    .w_full()
                    .bg(cx.theme().sidebar)  // 树背景
                    .child(
                        div()
                            .id("tree-scroll")
                            .flex_1()
                            .overflow_scroll()
                            .p_2()
                            .map(|this| {
                                if self.tree_state.read(cx).entries.is_empty() && !self.search_query.is_empty() {
                                    // 搜索无结果时显示空状态
                                    this.child(
                                        v_flex()
                                            .size_full()
                                            .items_center()
                                            .justify_center()
                                            .gap_3()
                                            .child(
                                                Icon::new(IconName::Search)
                                                    .with_size(Size::Large)
                                                    .text_color(cx.theme().muted_foreground)
                                            )
                                            .child(
                                                div()
                                                    .text_color(cx.theme().muted_foreground)
                                                    .child("未找到匹配项")
                                            )
                                    )
                                } else {
                                    this.child({
                                let view_for_click = view.clone();
                                let view_for_double_click = view.clone();

                                context_menu_tree(
                                    &self.tree_state,
                                    move |ix, item, _depth, selected, _window, cx| {
                                        let node_id = item.id.to_string();
                                        let (icon, label_text, label_for_tooltip, _item_clone, search_query, db_count, requires_double_click, is_folder_type) = view.update(cx, |this, cx| {
                                            let icon = this.get_icon_for_node(&node_id, item.is_expanded(),cx).color();

                                            // 获取节点类型，用于判断展开行为
                                            let node_type = this.db_nodes.get(&node_id).map(|n| n.node_type.clone());

                                            // 判断是否是分组类型（Folder 类型）
                                            let is_folder_type = matches!(
                                                node_type,
                                                Some(DbNodeType::TablesFolder) | Some(DbNodeType::ViewsFolder) |
                                                Some(DbNodeType::FunctionsFolder) | Some(DbNodeType::ProceduresFolder) |
                                                Some(DbNodeType::TriggersFolder) | Some(DbNodeType::QueriesFolder) |
                                                Some(DbNodeType::ColumnsFolder) | Some(DbNodeType::IndexesFolder)
                                            );

                                            // Connection、Database、Schema 只能通过双击展开，不响应箭头点击
                                            let requires_double_click = matches!(
                                                node_type,
                                                Some(DbNodeType::Connection) | Some(DbNodeType::Database) | Some(DbNodeType::Schema)
                                            );

                                            // 同步节点展开状态
                                            if item.is_expanded() && !requires_double_click {
                                                // 只有非双击展开的节点才同步展开状态
                                                this.expanded_nodes.insert(item.id.to_string());
                                            } else if !item.is_expanded() {
                                                this.expanded_nodes.remove(item.id.as_ref());
                                            }

                                            // 显示错误状态
                                            let error_msg = this.error_nodes.get(&node_id);

                                            // 文本始终保持为节点原始名称；错误信息只在 tooltip 中展示
                                            let label_text = item.label.to_string();

                                            let label_for_tooltip = if let Some(error) = error_msg {
                                                error.to_string()
                                            } else {
                                                label_text.clone()
                                            };

                                            let db_count = if node_type == Some(DbNodeType::Connection) {
                                                Some(this.get_selected_database_count(&node_id))
                                            } else {
                                                None
                                            };

                                            (icon, label_text, label_for_tooltip, item.clone(), this.search_query.clone(), db_count, requires_double_click, is_folder_type)
                                        });

                                        // 在 update 之后触发懒加载
                                        // Connection、Database、Schema 只能通过双击展开，这里不触发懒加载
                                        if item.is_expanded() && !requires_double_click {
                                            let id = node_id.clone();
                                            view.update(cx, |this, cx| {
                                                this.lazy_load_children(id, cx);
                                            });
                                        }

                                        // 创建 ListItem (不再添加 on_click，缩进由 context_menu_tree 处理)
                                        let view_clone = view.clone();
                                        let node_id_clone = node_id.clone();
                                        trace!("node_id: {}, item: {}", &node_id, &item.label);

                                        let (is_loading, error_msg, db_filter_list) = view.update(cx, |this, _cx| {
                                            let is_loading = this.loading_nodes.contains(&node_id);
                                            let error_msg = this.error_nodes.get(&node_id);
                                            let list_state = this.db_filter_list_states.get(&node_id).cloned();
                                            (is_loading, error_msg.cloned(), list_state)
                                        });

                                        let view_for_filter = view.clone();
                                        let node_id_for_filter = node_id.clone();

                                        // 选中状态的样式
                                        let selection_bg = cx.theme().sidebar_accent;
                                        let selection_bar_color = cx.theme().blue;
                                        let selection_text_color = cx.theme().sidebar_accent_foreground;
                                        let hover_bg = cx.theme().secondary;
                                        let folder_text_color = cx.theme().muted_foreground;
                                        let foreground_color = cx.theme().sidebar_foreground;

                                        // 使用 div 替代 ListItem 以精确控制样式
                                        let list_item = div()
                                            .id(SharedString::from(format!("tree-item-{}", ix)))
                                            .flex_1()
                                            .min_w(px(0.))
                                            .overflow_hidden()
                                            .h(px(26.))  // 行高 26px
                                            .relative()
                                            .flex()
                                            .items_center()
                                            .text_sm()
                                            .text_color(foreground_color)
                                            // 选中时显示左侧蓝条和背景
                                            .when(selected, |this| {
                                                this.child(
                                                    div()
                                                        .absolute()
                                                        .left_0()
                                                        .top_0()
                                                        .bottom_0()
                                                        .w(px(3.))  // 左侧选中条 3px
                                                        .bg(selection_bar_color)
                                                )
                                                .bg(selection_bg)
                                                .text_color(selection_text_color)
                                            })
                                            // hover 背景
                                            .when(!selected, |this| {
                                                this.hover(|style| style.bg(hover_bg))
                                            })
                                            .px_2()
                                            .child(
                                                h_flex()
                                                    .gap_2()
                                                    .items_center()
                                                    .min_w(px(0.))
                                                    .overflow_hidden()
                                                    .child(icon)
                                                    .child(
                                                        div()
                                                            .id(SharedString::from(format!("label-{}", ix)))
                                                            .flex_1()
                                                            .min_w(px(0.))
                                                            .overflow_hidden()
                                                            .whitespace_nowrap()
                                                            .text_ellipsis()
                                                            // 分组文字使用 muted.foreground（但选中状态下仍然白色）
                                                            .when(is_folder_type && !selected, |this| {
                                                                this.text_color(folder_text_color)
                                                            })
                                                            .child(Label::new(label_text).highlights(search_query).into_any_element())
                                                            .tooltip(move |window, cx| {
                                                                Tooltip::new(label_for_tooltip.clone()).build(window, cx)
                                                            })
                                                    )
                                                    .when_some(db_count, |this, (selected, total)| {
                                                        if total > 0 {
                                                            let view_open = view_for_filter.clone();
                                                            let node_id_open = node_id_for_filter.clone();

                                                            this.child(
                                                                Popover::new(SharedString::from(format!("db-filter-{}", ix)))
                                                                    .on_open_change(move |open, window, cx| {
                                                                        if *open {
                                                                            view_open.update(cx, |this, cx| {
                                                                                let databases_data = this.get_databases_for_connection(&node_id_open);

                                                                                if let Some(list_state) = this.db_filter_list_states.get(&node_id_open) {
                                                                                    list_state.update(cx, |state, _| {
                                                                                        let delegate = state.delegate_mut();
                                                                                        delegate.databases = databases_data.clone();
                                                                                        delegate.filtered_databases = databases_data;
                                                                                    });
                                                                                } else {
                                                                                    let list_state = cx.new(|cx| {
                                                                                        ListState::new(
                                                                                            DatabaseListDelegate::new(
                                                                                                view_open.clone(),
                                                                                                node_id_open.clone(),
                                                                                                databases_data.clone(),
                                                                                            ),
                                                                                            window,
                                                                                            cx,
                                                                                        )
                                                                                        .searchable(true)
                                                                                    });
                                                                                    this.db_filter_list_states.insert(node_id_open.clone(), list_state);
                                                                                }
                                                                                cx.notify();
                                                                            });
                                                                        }
                                                                    })
                                                                    .when_some(db_filter_list.as_ref(), |popover, list| {
                                                                        popover.track_focus(&list.focus_handle(cx))
                                                                    })
                                                                    .trigger(
                                                                        Button::new(SharedString::from(format!("db-filter-trigger-{}", ix)))
                                                                            .ghost()
                                                                            .small()
                                                                            .label(format!("{} of {}", selected, total))
                                                                    )
                                                                    .when_some(db_filter_list, |popover, list| {
                                                                        let view_content = view_for_filter.clone();
                                                                        let node_id_content = node_id_for_filter.clone();
                                                                        popover.content(move |_state, _window, cx| {
                                                                            Self::render_database_filter_popover(
                                                                                &view_content,
                                                                                &node_id_content,
                                                                                &list,
                                                                                cx,
                                                                            )
                                                                        })
                                                                    })
                                                            )
                                                        } else {
                                                            this
                                                        }
                                                    })
                                                    .when(is_loading, |this| {
                                                        this.child(
                                                            Spinner::new()
                                                                .with_size(Size::Small)
                                                                .color(cx.theme().muted_foreground)
                                                        )
                                                    })
                                                    .when_some(error_msg.clone(), |this, error_text| {
                                                        let error_for_copy = error_text.clone();
                                                        this.child(
                                                            Popover::new(SharedString::from(format!("error-popover-{}", ix)))
                                                                .trigger(
                                                                    Button::new(SharedString::from(format!("error-btn-{}", ix)))
                                                                        .ghost()
                                                                        .icon(IconName::TriangleAlert)
                                                                        .xsmall()
                                                                        .text_color(cx.theme().warning)
                                                                ).content(move |_state, _window, cx| {
                                                                    let error_for_copy = error_for_copy.clone();
                                                                    v_flex()
                                                                        .gap_2()
                                                                        .child(
                                                                            h_flex()
                                                                                .items_center()
                                                                                .justify_between()
                                                                                .child(
                                                                                    h_flex()
                                                                                        .items_center()
                                                                                        .gap_1()
                                                                                        .child(
                                                                                            Icon::new(IconName::TriangleAlert)
                                                                                                .with_size(Size::Small)
                                                                                                .text_color(cx.theme().warning)
                                                                                        )
                                                                                        .child("错误信息")
                                                                                )
                                                                                .child(
                                                                                    Clipboard::new(SharedString::from(format!("copy-error-{}", ix)))
                                                                                        .value(error_for_copy)
                                                                                )
                                                                        )
                                                                    .child(
                                                                        div()
                                                                            .text_sm()
                                                                            .text_color(cx.theme().muted_foreground)
                                                                            .child(error_text.clone())
                                                                    )
                                                            })
                                                                .max_w(px(400.))
                                                        )
                                                    })
                                            );

                                        // 使用 context_menu 方法为 ListItem 添加上下文菜单
                                        list_item
                                            .context_menu(move |menu, window, cx| {
                                                        // 从 db_nodes 获取节点信息
                                                        if let Some(node) = view_clone.read(cx).db_nodes.get(&node_id_clone).cloned() {
                                                            let mut menu = menu;
                                                            // 根据节点类型添加不同的菜单项
                                                            match node.node_type {
                                                                DbNodeType::Connection => {
                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_clone, "运行SQL文件".to_string(), &view_clone, window, |n| DbTreeViewEvent::RunSqlFile { node_id: n }))
                                                                        .separator()
                                                                        .item(Self::create_menu_item(&node_id_clone, "关闭连接".to_string(), &view_clone, window, |n| DbTreeViewEvent::CloseConnection { node_id: n }))
                                                                        .separator()
                                                                        .item(Self::create_menu_item(&node_id_clone, "删除连接".to_string(), &view_clone, window, |n| DbTreeViewEvent::DeleteConnection { node_id: n }))
                                                                        .separator()
                                                                        .item(Self::create_menu_item(&node_id_clone, "新建数据库".to_string(), &view_clone, window, |n| DbTreeViewEvent::CreateDatabase { node_id: n }))
                                                                }
                                                                DbNodeType::Database => {
                                                                    let node_id_for_menu = node_id_clone.clone();

                                                                    let capabilities = {
                                                                        let registry = cx.global::<DatabaseViewPluginRegistry>();
                                                                        registry.get(&node.database_type)
                                                                            .map(|p| p.get_node_menu_capabilities())
                                                                            .unwrap_or_default()
                                                                    };

                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "新建查询".to_string(), &view_clone, window, |n| DbTreeViewEvent::CreateNewQuery { node_id: n.clone() }))
                                                                        .separator()
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "运行SQL文件".to_string(), &view_clone, window, |n| DbTreeViewEvent::RunSqlFile { node_id: n.clone() }));

                                                                    if capabilities.supports_dump_database {
                                                                        menu = menu.submenu("转储SQL文件", window, cx, {
                                                                            let view_submenu = view_clone.clone();
                                                                            let node_id_submenu = node_id_for_menu.clone();
                                                                            move |menu, window, _cx| {
                                                                                menu
                                                                                    .item(
                                                                                        PopupMenuItem::new("导出结构")
                                                                                            .on_click(window.listener_for(&view_submenu, {
                                                                                                let node_id = node_id_submenu.clone();
                                                                                                move |_this, _, _, cx| {
                                                                                                    cx.emit(DbTreeViewEvent::DumpSqlFile {
                                                                                                        node_id: node_id.clone(),
                                                                                                        mode: SqlDumpMode::StructureOnly,
                                                                                                    });
                                                                                                }
                                                                                            }))
                                                                                    )
                                                                                    .item(
                                                                                        PopupMenuItem::new("导出数据")
                                                                                            .on_click(window.listener_for(&view_submenu, {
                                                                                                let node_id = node_id_submenu.clone();
                                                                                                move |_this, _, _, cx| {
                                                                                                    cx.emit(DbTreeViewEvent::DumpSqlFile {
                                                                                                        node_id: node_id.clone(),
                                                                                                        mode: SqlDumpMode::DataOnly,
                                                                                                    });
                                                                                                }
                                                                                            }))
                                                                                    )
                                                                                    .item(
                                                                                        PopupMenuItem::new("导出结构和数据")
                                                                                            .on_click(window.listener_for(&view_submenu, {
                                                                                                let node_id = node_id_submenu.clone();
                                                                                                move |_this, _, _, cx| {
                                                                                                    cx.emit(DbTreeViewEvent::DumpSqlFile {
                                                                                                        node_id: node_id.clone(),
                                                                                                        mode: SqlDumpMode::StructureAndData,
                                                                                                    });
                                                                                                }
                                                                                            }))
                                                                                    )
                                                                            }
                                                                        });
                                                                    }

                                                                    menu = menu.separator();

                                                                    if capabilities.supports_edit_database {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "编辑数据库".to_string(), &view_clone, window, |n| DbTreeViewEvent::EditDatabase { node_id: n.clone() }));
                                                                    }
                                                                    if capabilities.supports_create_schema {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "新建模式".to_string(), &view_clone, window, |n| DbTreeViewEvent::CreateSchema { node_id: n.clone() }));
                                                                    }
                                                                    menu = menu.item(Self::create_menu_item(&node_id_for_menu, "关闭数据库".to_string(), &view_clone, window, |n| DbTreeViewEvent::CloseDatabase { node_id: n.clone() }));
                                                                    if capabilities.supports_drop_database {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "删除数据库".to_string(), &view_clone, window, |n| DbTreeViewEvent::DeleteDatabase { node_id: n.clone() }));
                                                                    }

                                                                    menu = menu.separator()
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "导入数据".to_string(), &view_clone, window, |n| DbTreeViewEvent::ImportData { node_id: n.clone() }))
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "导出数据库".to_string(), &view_clone, window, |n| DbTreeViewEvent::ExportData { node_id: n }))
                                                                        .separator();
                                                                }
                                                                DbNodeType::Table => {
                                                                    let node_id_for_menu = node_id_clone.clone();

                                                                    let capabilities = {
                                                                        let registry = cx.global::<DatabaseViewPluginRegistry>();
                                                                        registry.get(&node.database_type)
                                                                            .map(|p| p.get_node_menu_capabilities())
                                                                            .unwrap_or_default()
                                                                    };

                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "查看表数据".to_string(), &view_clone, window, |n| DbTreeViewEvent::OpenTableData { node_id: n.clone() }))
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "设计表".to_string(), &view_clone, window, |n| DbTreeViewEvent::DesignTable { node_id: n.clone() }))
                                                                        .separator();

                                                                    if capabilities.supports_rename_table {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "重命名表".to_string(), &view_clone, window, |n| DbTreeViewEvent::RenameTable { node_id: n.clone() }));
                                                                    }
                                                                    if capabilities.supports_truncate_table {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "清空表".to_string(), &view_clone, window, |n| DbTreeViewEvent::TruncateTable { node_id: n.clone() }));
                                                                    }
                                                                    menu = menu.item(Self::create_menu_item(&node_id_for_menu, "删除表".to_string(), &view_clone, window, |n| DbTreeViewEvent::DeleteTable { node_id: n.clone() }))
                                                                        .separator();

                                                                    if capabilities.supports_table_import {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "导入数据".to_string(), &view_clone, window, |n| DbTreeViewEvent::ImportData { node_id: n.clone() }));
                                                                    }
                                                                    if capabilities.supports_table_export {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "导出表".to_string(), &view_clone, window, |n| DbTreeViewEvent::ExportData { node_id: n }));
                                                                    }
                                                                    menu = menu.separator();
                                                                }
                                                                DbNodeType::View => {
                                                                    let node_id_for_menu = node_id_clone.clone();

                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "查看视图数据".to_string(), &view_clone, window, |n| DbTreeViewEvent::OpenViewData { node_id: n.clone() }))
                                                                        .separator()
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "删除视图".to_string(), &view_clone, window, |n| DbTreeViewEvent::DeleteView { node_id: n }))
                                                                        .separator();
                                                                }
                                                                DbNodeType::Schema => {
                                                                    let node_id_for_menu = node_id_clone.clone();

                                                                    let capabilities = {
                                                                        let registry = cx.global::<DatabaseViewPluginRegistry>();
                                                                        registry.get(&node.database_type)
                                                                            .map(|p| p.get_node_menu_capabilities())
                                                                            .unwrap_or_default()
                                                                    };

                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "新建查询".to_string(), &view_clone, window, |n| DbTreeViewEvent::CreateNewQuery { node_id: n.clone() }))
                                                                        .separator();

                                                                    if capabilities.supports_delete_schema {
                                                                        menu = menu.item(Self::create_menu_item(&node_id_for_menu, "删除模式".to_string(), &view_clone, window, |n| DbTreeViewEvent::DeleteSchema { node_id: n.clone() }))
                                                                            .separator();
                                                                    }
                                                                }
                                                                DbNodeType::QueriesFolder => {
                                                                    let node_id_for_menu = node_id_clone.clone();

                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "新建查询".to_string(), &view_clone, window, |n| DbTreeViewEvent::CreateNewQuery { node_id: n.clone() }))
                                                                        .separator()
                                                                        .item(PopupMenuItem::new("刷新")
                                                                            .on_click(window.listener_for(&view_clone, move |this, _, _, cx| {
                                                                                this.refresh_tree(node_id_for_menu.clone(), cx);
                                                                            }))
                                                                        )
                                                                        .separator();
                                                                }
                                                                DbNodeType::NamedQuery => {
                                                                    let node_id_for_menu = node_id_clone.clone();

                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "打开查询".to_string(), &view_clone, window, |n| DbTreeViewEvent::OpenNamedQuery { node_id: n.clone() }))
                                                                        .separator()
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "重命名查询".to_string(), &view_clone, window, |n| DbTreeViewEvent::RenameQuery { node_id: n.clone() }))
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "删除查询".to_string(), &view_clone, window, |n| DbTreeViewEvent::DeleteQuery { node_id: n }))
                                                                        .separator();
                                                                }
                                                                DbNodeType::TablesFolder => {
                                                                    let node_id_for_menu = node_id_clone.clone();

                                                                    menu = menu
                                                                        .item(Self::create_menu_item(&node_id_for_menu, "新建表".to_string(), &view_clone, window, |n| DbTreeViewEvent::DesignTable { node_id: n.clone() }))
                                                                        .separator();
                                                                }
                                                                _ => {}
                                                            }

                                                            let view_ref2 = view_clone.clone();
                                                            let id_clone = node_id_clone.clone();
                                                            menu.item(
                                                                PopupMenuItem::new("刷新")
                                                                    .on_click(window.listener_for(&view_ref2, move |this, _, _, cx| {
                                                                        this.refresh_tree(id_clone.clone(), cx);
                                                                    }))
                                                            )
                                                        } else {
                                                            menu
                                                        }
                                            })
                                            .into_any_element()
                                    },
                                )
                                .on_click({
                                    move |_ix, item, cx| {
                                        view_for_click.update(cx, |this, cx| {
                                           this.handle_item_click(item.clone(), cx)
                                        });
                                    }
                                })
                                .on_double_click({
                                    move |_ix, item, cx| {
                                        view_for_double_click.update(cx, |this, cx| {
                                            this.handle_item_double_click(item.clone(), cx);
                                        });
                                    }
                                })
                            })
                                }
                            })
                    )
            )
    }
}

impl EventEmitter<DbTreeViewEvent> for DbTreeView {}


impl Focusable for DbTreeView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

