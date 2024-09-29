local skynet = require "skynet"
local service = require "skynet.service"
local core = require "skynet.sharetable.core"
local is_sharedtable = core.is_sharedtable
local stackvalues = core.stackvalues

local function sharetable_service()
	local skynet = require "skynet"
	local core = require "skynet.sharetable.core"

	local matrix = {}	-- all the matrix
	local files = {}	-- filename : matrix
	local clients = {}

	local sharetable = {}

	local function close_matrix(m)
		if m == nil then
			return
		end
		local ptr = m:getptr()
		local ref = matrix[ptr]
		if ref == nil or ref.count == 0 then
			matrix[ptr] = nil
			m:close()
		end
	end

	function sharetable.loadfile(source, filename, ...)
		close_matrix(files[filename])
		local m = core.matrix("@" .. filename, ...)
		files[filename] = m
		skynet.ret()
	end

	function sharetable.loadstring(source, filename, datasource, ...)
		close_matrix(files[filename])
		local m = core.matrix(datasource, ...)
		files[filename] = m
		skynet.ret()
	end

	local function loadtable(filename, ptr, len)

		close_matrix(files[filename])

        -- 这是啥意思呢。 必须要放到c环境下去处理吗？？？？？
		local m = core.matrix([[
			local unpack, ptr, len = ...
			return unpack(ptr, len)
		]], skynet.unpack, ptr, len)
		files[filename] = m
	end

	function sharetable.loadtable(source, filename, ptr, len)
		local ok, err = pcall(loadtable, filename, ptr, len)
		skynet.trash(ptr, len)
		assert(ok, err)
		skynet.ret()
	end

	local function query_file(source, filename)
		local m = files[filename]
		local ptr = m:getptr()
		local ref = matrix[ptr]
		if ref == nil then
			ref = {
				filename = filename,
				count = 0,
				matrix = m,
				refs = {},
			}
			matrix[ptr] = ref
		end
		if ref.refs[source] == nil then
			ref.refs[source] = true
			local list = clients[source]
			if not list then
				clients[source] = { ptr }
			else
				table.insert(list, ptr)
			end
			ref.count = ref.count + 1
		end
		return ptr
	end

	function sharetable.query(source, filename)
		local m = files[filename]
		if m == nil then
			skynet.ret()
			return
		end
		local ptr = query_file(source, filename)
		skynet.ret(skynet.pack(ptr))
	end

	local function querylist(source, filenamelist)
		local ptrList = {}
        for _, filename in ipairs(filenamelist) do
            if files[filename] then
                ptrList[filename] = query_file(source, filename)
            end
        end
		return ptrList
	end

	local function queryall(source)
		local ptrList = {}
		for filename in pairs(files) do
			ptrList[filename] = query_file(source, filename)
		end
		return ptrList
	end

    function sharetable.queryall(source, filenamelist)
		local queryFunc = filenamelist and querylist or queryall
		local ptrList = queryFunc(source, filenamelist)
        skynet.ret(skynet.pack(ptrList))
    end

	function sharetable.close(source)
		local list = clients[source]
		if list then
			for _, ptr in ipairs(list) do
				local ref = matrix[ptr]
				if ref and ref.refs[source] then
					ref.refs[source] = nil
					ref.count = ref.count - 1
					if ref.count == 0 then
						if files[ref.filename] ~= ref.matrix then
							-- It's a history version
							skynet.error(string.format("Delete a version (%s) of %s", ptr, ref.filename))
							ref.matrix:close()
							matrix[ptr] = nil
						end
					end
				end
			end
			clients[source] = nil
		end
		-- no return
	end

	skynet.dispatch("lua", function(_,source,cmd,...)
		sharetable[cmd](source,...)
	end)

	skynet.info_func(function()
		local info = {}

		for filename, m in pairs(files) do
			info[filename] = {
				current = m:getptr(),
				size = m:size(),
			}
		end

		local function address(refs)
			local keys = {}
			for addr in pairs(refs) do
				table.insert(keys, skynet.address(addr))
			end
			table.sort(keys)
			return table.concat(keys, ",")
		end

		for ptr, copy in pairs(matrix) do
			local v = info[copy.filename]
			local h = v.history
			if h == nil then
				h = {}
				v.history = h
			end
			table.insert(h, string.format("%s [%d]: (%s)", copy.matrix:getptr(), copy.matrix:size(), address(copy.refs)))
		end
		for _, v in pairs(info) do
			if v.history then
				v.history = table.concat(v.history, "\n\t")
			end
		end

		return info
	end)

end

local function load_service(t, key)
	if key == "address" then
		t.address = service.new("sharetable", sharetable_service)
		return t.address
	else
		return nil
	end
end

local function report_close(t)
	local addr = rawget(t, "address")
	if addr then
		skynet.send(addr, "lua", "close")
	end
end

local sharetable = setmetatable ( {} , {
	__index = load_service,
	__gc = report_close,
})

function sharetable.loadfile(filename, ...)
	skynet.call(sharetable.address, "lua", "loadfile", filename, ...)
end

function sharetable.loadstring(filename, source, ...)
	skynet.call(sharetable.address, "lua", "loadstring", filename, source, ...)
end

function sharetable.loadtable(filename, tbl)
	assert(type(tbl) == "table")
	skynet.call(sharetable.address, "lua", "loadtable", filename, skynet.pack(tbl))
end


local RECORD = {}
function sharetable.query(filename)
    -- 获取指针？
	local newptr = skynet.call(sharetable.address, "lua", "query", filename)
	if newptr then
        -- clone table
		local t = core.clone(newptr)
		local map = RECORD[filename]
		if not map then
			map = {}
			RECORD[filename] = map
		end
        -- 这个t是 数据？？
		map[t] = true
		return t
	end
end

function sharetable.queryall(filenamelist)
    local list, t, map = {}
    local ptrList = skynet.call(sharetable.address, "lua", "queryall", filenamelist)
    for filename, ptr in pairs(ptrList) do
        t = core.clone(ptr)
        map = RECORD[filename]
        if not map then
            map = {}
            RECORD[filename] = map
        end
        map[t] = true
        list[filename] = t
    end
    return list
end

local pairs = pairs
local type = type
local assert = assert
local next = next
local rawset = rawset
local getuservalue = debug.getuservalue
local setuservalue = debug.setuservalue
local getupvalue = debug.getupvalue
local setupvalue = debug.setupvalue
local getlocal = debug.getlocal
local setlocal = debug.setlocal
local getinfo = debug.getinfo

local NILOBJ = {}
local function insert_replace(old_t, new_t, replace_map)
    -- 如果old_t不是map呢
    -- 当是table，才进行替换
    for k, ov in pairs(old_t) do
        -- 如果ov 不是table，就不走这个逻辑
        if type(ov) == "table" then
            local nv = new_t[k]
            if nv == nil then
                nv = NILOBJ
            end
            assert(replace_map[ov] == nil)
            -- 这一行是为确保上一行的执行吧？。不会递归？
            replace_map[ov] = nv
            nv = type(nv) == "table" and nv or NILOBJ
            insert_replace(ov, nv, replace_map)
        end
    end
    --如果old_t 不是table， 只会走这里
    --当然如果old_t是table，也会走这里
    replace_map[old_t] = new_t
    return replace_map
end



-- This function is designed to replace values within a given Lua table based on a provided replacement map (replace_map).
-- It iterates through the table and its nested structures, recursively applying replacements as needed.

-- 这段代码的主要目的是在Lua环境中提供一种灵活的数据替换机制，能够处理包括表、函数、userdata以及线程（协程）在内的多种数据类型。
-- 通过replace_map，用户可以指定哪些值需要被替换为新的值，同时，它还能够处理这些值的元表以及函数的上值和局部变量。
-- 这种机制在需要深度修改或替换Lua环境中数据结构的场景中非常有用。
-- 定义一个函数，用于根据replace_map替换值
local function resolve_replace(replace_map)
    -- 初始化一个空表，用于存储类型对应的匹配函数
    -- match: A table used to store functions for matching different value types (e.g., number, string, table).
    local match = {}
    -- 初始化一个空表，用于记录已经处理过的值，防止循环引用
    -- record_map: A table used to keep track of values that have already been processed to avoid infinite recursion.
    local record_map = {}

    -- 辅助函数，用于从replace_map中获取替换后的值，处理NILOBJ特殊情况
    -- Retrieves the replacement value for a given key from the replace_map.
    -- Handles the case where the replacement value is NILOBJ, indicating that the value should be replaced with nil.
    local function getnv(v)
        local nv = replace_map[v]
        if nv then
            if nv == NILOBJ then   -- 如果指定替换为NILOBJ，则返回nil
                return nil
            end
            return nv
        end
        assert(false)   -- 如果v在replace_map中没有找到且不是NILOBJ，则断言失败
    end

    -- Checks if the value has already been processed or is a shared table (to avoid infinite recursion).
    -- Retrieves the appropriate matching function based on the value's type and applies it.
    -- 对值进行匹配和替换，如果是nil、已记录或共享表，则不处理
    local function match_value(v)
        if v == nil or record_map[v] or is_sharedtable(v) then
            return
        end

        -- 根据值的类型调用相应的匹配函数
        local tv = type(v)
        local f = match[tv]
        if f then
            record_map[v] = true
            return f(v)
        end
    end
    -- 对值的元表进行匹配和替换
    local function match_mt(v)
        -- 如果v的元表在replace_map中有替换项，则更新元表
        local mt = debug.getmetatable(v)
        if mt then
            local nv = replace_map[mt]
            if nv then
                nv = getnv(mt)
                debug.setmetatable(v, nv)
            else
                return match_value(mt)
            end
        end
    end
    -- 对内部类型（如函数参数、布尔值等）的元表进行匹配
    local function match_internmt()
        -- 这个应该是获取各个类型而已。值不重要？
        local internal_types = {
            -- 返回指定函数第 n 个上值的唯一标识符（一个轻量用户数据）。
            -- 这个唯一标识符可以让程序检查两个不同的闭包是否共享了上值。 若 Lua 闭包之间共享的是同一个上值 （即指向一个外部局部变量），会返回相同的标识符。
            -- _ENV 被 _ENV 用于值的那张表被称为 环境。
            -- 当 Lua 加载一个代码块，_ENV 这个上值的默认值就是这个全局环境
            pointer = debug.upvalueid(getnv, 1),
            boolean = false,
            str = "",
            number = 42,
            thread = coroutine.running(),
            func = getnv,
        }
        -- 遍历一系列内部类型并尝试匹配其元表
        for _,v in pairs(internal_types) do
            match_mt(v)
        end
        return match_mt(nil)
    end

    -- 对表进行遍历，替换其中的键和值，并处理元表
    local function match_table(t)
        local keys = false

        -- 对表的每个键值对进行处理，如果键或值在replace_map中有替换项，则更新

        for k,v in next, t do
            local tk = type(k)
            if match[tk] then
                keys = keys or {}
                keys[#keys+1] = k
            end

            local nv = replace_map[v]
            if nv then
                nv = getnv(v)
                rawset(t, k, nv)
            else
                match_value(v)
            end
        end

        if keys then
            for _, old_k in ipairs(keys) do
                local new_k = replace_map[old_k]
                if new_k then
                    local value = rawget(t, old_k)
                    new_k = getnv(old_k)
                    rawset(t, old_k, nil)
                    if new_k then
                        rawset(t, new_k, value)
                    end
                else
                    match_value(old_k)
                end
            end
        end
        return match_mt(t)
    end
    -- 对userdata类型的值进行处理，主要是替换其用户值（uservalue）和元表
    local function match_userdata(u)
        local uv = getuservalue(u)
        local nv = replace_map[uv]
        -- 如果userdata的用户值在replace_map中有替换项，则更新用户值
        if nv then
            nv = getnv(uv)
            setuservalue(u, nv)
        end
        return match_mt(u)
    end
    -- 对函数信息进行处理，包括其上值（upvalue）和局部变量（local variable）
    local function match_funcinfo(info)
        local func = info.func
        local nups = info.nups

        -- 遍历函数的每个上值和局部变量，进行替换
        for i=1,nups do
            local name, upv = getupvalue(func, i)
            local nv = replace_map[upv]
            if nv then
                nv = getnv(upv)
                setupvalue(func, i, nv)
            elseif upv then
                match_value(upv)
            end
        end

        local level = info.level
        local curco = info.curco
        if not level then
            return
        end
        local i = 1
        while true do
            local name, v = getlocal(curco, level, i)
            if name == nil then
                break
            end
            if replace_map[v] then
                local nv = getnv(v)
                setlocal(curco, level, i, nv)
            elseif v then
                match_value(v)
            end
            i = i + 1
        end
    end
    -- 对函数进行处理，实际上是调用match_funcinfo
    local function match_function(f)
        local info = getinfo(f, "uf")
        return match_funcinfo(info)
    end
    -- 对线程（协程）的栈值和函数信息进行匹配处理
    local function match_thread(co, level)
        -- match stackvalues
        local values = {}
        local n = stackvalues(co, values)
        for i=1,n do
            local v = values[i]
            match_value(v)
        end

        local uplevel = co == coroutine.running() and 1 or 0
        level = level or 1
        -- 遍历线程的栈值和函数信息，进行替换
        while true do
            local info = getinfo(co, level, "uf")
            if not info then
                break
            end
            info.level = level + uplevel
            info.curco = co
            match_funcinfo(info)
            level = level + 1
        end
    end
    -- 初始化匹配过程，记录一些特殊的值，并忽略部分栈帧
    local function prepare_match()

        -- 主要是防止对特定值和函数进行重复处理

        local co = coroutine.running()
        record_map[co] = true
        record_map[match] = true
        record_map[RECORD] = true
        record_map[record_map] = true
        record_map[replace_map] = true
        record_map[insert_replace] = true
        record_map[resolve_replace] = true
        assert(getinfo(co, 3, "f").func == sharetable.update)
        match_thread(co, 5) -- ignore match_thread and match_funcinfo frame
    end

    -- 设置match表，使其包含对不同类型值的匹配函数
    match["table"] = match_table
    match["function"] = match_function
    match["userdata"] = match_userdata
    match["thread"] = match_thread

    -- 调用prepare_match进行初始化，并对内部类型的元表进行匹配
    prepare_match()
    match_internmt()

    -- 最后，对Lua注册表（全局环境）进行匹配处理
    --Lua 提供了一个 注册表， 这是一个预定义出来的表， 可以用来保存任何 C 代码想保存的 Lua 值。 这个表可以用有效伪索引 LUA_REGISTRYINDEX 来定位。 任何 C 库都可以在这张表里保存数据， 为了防止冲突，你需要特别小心的选择键名。 一般的用法是，你可以用一个包含你的库名的字符串做为键名， 或者取你自己 C 对象的地址，以轻量用户数据的形式做键， 还可以用你的代码创建出来的任意 Lua 对象做键。 关于变量名，字符串键名中以下划线加大写字母的名字被 Lua 保留。
    --
    --注册表中的整数键用于引用机制 （参见 luaL_ref）， 以及一些预定义的值。 因此，整数键不要用于别的目的。
    --
    --当你创建了一个新的 Lua 状态机， 其中的注册表内就预定义好了几个值。 这些预定义值可以用整数索引到， 这些整数以常数形式定义在 lua.h 中。 有下列常数：
    --
    --LUA_RIDX_MAINTHREAD: 注册表中这个索引下是状态机的主线程。 （主线程和状态机同时被创建出来。）
    --LUA_RIDX_GLOBALS: 注册表的这个索引下是全局环境。
    local root = debug.getregistry()
    assert(replace_map[root] == nil)    -- 确保replace_map中没有对注册表的替换项

    -- 接下来的代码被截断了，但预期是对root进行match_table处理
    match_table(root)
end

-- 为什么update搞得这么费劲。目的是什么
-- 我们之前的配置更新是怎么处理呢
-- 就是直接替换索引。 那之前的配置被本地引用了。那是特殊情况或代码实现问题
function sharetable.update(...)
	local names = {...}
	local replace_map = {}
	for _, name in ipairs(names) do
		local map = RECORD[name]
		if map then
            -- 新值
			local new_t = sharetable.query(name)
            -- old_t 是值。应该是个map
			for old_t,_ in pairs(map) do
				if old_t ~= new_t then
                    -- 先做映射关系，old_v 映射到new_v
					insert_replace(old_t, new_t, replace_map)
                    -- 这更新过程中有获取呢？？
                    map[old_t] = nil
				end
			end
		end
	end

    if next(replace_map) then
        resolve_replace(replace_map)
    end
end

return sharetable

