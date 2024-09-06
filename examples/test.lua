



--assert(true, "111111111111111")
--
--
--local mm = {}
--local ll = {}
--
--print("case1 : " .. tostring(mm == mm))
--print("case2 : " .. tostring(mm == {}))
--print("case3 : " .. tostring(mm == ll))
--
--local m = 1;
--local d = 2
--
--
--print(m & 0xff)



local function toBinary(n)
    local binary = ""
    while n > 0 do
        local bit = n % 2
        binary = tostring(bit) .. binary
        n = math.floor(n / 2)
    end
    return binary
end


print(toBinary(0xf))

print(0xf)
