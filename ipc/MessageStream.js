// 兼容垫片：nativeService.js 按 modules/ipc 布局引用 './ipc/MessageStream.js'
// 本仓库根目录为扁平布局，这里统一转出同一模块实例（不会产生重复类）
export { MessageStream } from '../MessageStream.js';
