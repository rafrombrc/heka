-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

function process_message ()
    local msg = decode_message(read_message("raw"))
    if msg.Type ~= "TEST" then return 1 end
    if msg.Logger ~= "GoSpec" then return 2 end
    if msg.Payload ~= "Payload Test" then return 3 end
    local foo_values = {}
    for _, f in ipairs(msg.Fields) do
        if f.name == "bool" and (#f.value ~= 1 or f.value[1] ~= true) then return 4 end
        if f.name == "int" then
            if #f.value ~= 2 or f.value[1] ~= 999 or f.value[2] ~= 1024 then return 5 end
        end
        if f.name == "double" and (#f.value ~= 1 or f.value[1] ~= 99.9) then return 6 end
        if f.name == "foo" then
            if #f.value ~= 1 then return 7 end
            foo_values[#foo_values+1] = f.value[1]
        end
    end
    if #foo_values ~= 2 then return 8 end
    if foo_values[1] ~= "bar" or foo_values[2] ~= "alternate" then return 9 end
    return 0
end
