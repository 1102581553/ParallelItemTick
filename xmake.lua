set_project("ParallelItemTick")
set_version("1.0.0")

-- 添加 LeviLamina 官方插件仓库
add_repositories("levimc-repo https://github.com/LiteLDev/xmake-repo.git")

-- 声明依赖项
-- 指定使用 levilamina 1.9.5 版本，目标平台为服务端 [cite: 137]
add_requires("levilamina 1.9.5", {configs = {target_type = "server"}})
-- 添加构建脚本依赖，用于后续的 linkrule 和 modpacker 
add_requires("levibuildscript") 

-- 运行时配置
if not has_config("vs_runtime") then
    set_runtimes("MD")
end

-- 编译选项设置
if is_plat("windows") then
    add_cxflags(
        "/EHa", "/utf-8", "/W4",
        "/w44265", "/w44289", "/w44296", "/w45263", "/w44738", "/w45204"
    )
else
    -- 非 Windows 平台（如使用 Wine 环境编译）的优化参数
    add_cxflags("-O2", "-march=native", "-flto=auto")
end

-- 全局宏定义与语言标准
add_defines("NOMINMAX", "UNICODE")
set_languages("c++20") -- 模组开发建议使用 C++20 标准 
set_optimize("fastest") -- 开启极致优化以提升并行 Tick 效率 
set_symbols("none")
set_exceptions("none")

-- 包含目录
add_includedirs("src")

-- 目标模组配置
target("ParallelItemTick")
    -- 应用 LeviLamina 专属构建规则 
    add_rules("@levibuildscript/linkrule")
    add_rules("@levibuildscript/modpacker")
    
    set_kind("shared") -- 模组必须编译为动态链接库 (DLL) [cite: 124]
    
    -- 文件包含范围
    add_headerfiles("src/*.h")
    add_files("src/*.cpp")
    
    -- 链接依赖包
    add_packages("levilamina")
    -- 如果你的代码中直接使用了 JSON 反射，通常需要包含此包
    add_packages("nlohmann_json") 
    
    -- 系统底层库链接
    add_syslinks("shlwapi", "advapi32")
    
    -- 输出目录配置
    set_targetdir("bin")
    set_runtimes("MD")
