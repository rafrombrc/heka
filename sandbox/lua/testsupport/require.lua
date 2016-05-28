local constant = require "constant_module"

function process_message()
    inject_payload("", "", tostring(constant))
    return 0
end
