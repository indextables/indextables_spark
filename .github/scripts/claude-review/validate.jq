# Strict validation of the reviewer's structured output.
#
# Run with `jq -s -f validate.jq` so the input is the list of every JSON value
# found in the raw text. Emits the review object, rebuilt from the validated
# fields only, or fails with a fixed error text. Error texts never include any
# part of the input.
#
# The limits here are the ones in the --json-schema passed to the reviewer in
# .github/workflows/claude-review.yml. Keep the two in step; test.sh compares them.

def limits: {summary: 600, findings: 30, path: 300, message: 500, line: 9999999};
def severities: ["critical", "high", "medium", "low"];

def only_keys($allowed): (keys - $allowed) == [];
def bounded_string($min; $max): type == "string" and length >= $min and length <= $max;

def valid_finding:
  type == "object"
  and only_keys(["severity", "path", "line", "message"])
  and (.severity as $s | ($s | type) == "string" and (severities | index($s)) != null)
  and (.path | bounded_string(1; limits.path))
  and (.message | bounded_string(1; limits.message))
  and ((has("line") | not)
       or .line == null
       or ((.line | type) == "number" and .line == (.line | floor)
           and .line >= 1 and .line <= limits.line));

if length != 1 then error("expected exactly one JSON value")
else .[0]
| if type != "object" then error("top level is not an object")
  elif (only_keys(["verdict", "summary", "findings"]) | not) then error("unexpected top-level field")
  elif (.verdict != "pass" and .verdict != "fail") then error("verdict is not pass or fail")
  elif (.summary | bounded_string(0; limits.summary) | not) then error("summary missing, not a string, or too long")
  elif (.findings | type) != "array" then error("findings is not an array")
  elif (.findings | length) > limits.findings then error("too many findings")
  elif (.findings | all(.[]; valid_finding) | not) then error("a finding does not match the schema")
  else {
    verdict,
    summary,
    findings: [.findings[] | {severity, path, line: (.line // null), message}]
  }
  end
end
