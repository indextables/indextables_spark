# Render the pull request comment / job summary for one review outcome.
#
# Run with `jq -n -r -f render.jq` and:
#   --arg status       pass | fail | none
#   --arg reason_text  fixed explanation, used when status is none
#   --arg note         fixed extra sentence, may be empty
#   --argjson review   output of validate.jq, or null
#   --arg pr, --arg sha, --arg run_url
#   --argjson max_chars  upper bound for the whole body
#
# Every string that originates from the reviewer goes through `code`: reduced
# to one line of printable ASCII without backticks and wrapped in a code span.
# Inside a code span GitHub renders no mentions, links, images or HTML, and
# without a backtick or a line break the span cannot be closed early.

def clean:
  gsub("[\\t\\n\\r]+"; " ")
  | gsub("[^ -~]"; "?")
  | gsub("`"; "'")
  | gsub(" {2,}"; " ")
  | sub("^ "; "") | sub(" $"; "")
  | if . == "" then "(empty)" else . end;

def code: "`" + clean + "`";

def location: .path + (if .line == null then "" else ":" + (.line | tostring) end);

def heading:
  if $status == "pass" then "PASS"
  elif $status == "fail" then "FAIL"
  else "DID NOT COMPLETE" end;

def body($shown):
  ($review.findings // []) as $all
  | ([
      "<!-- claude-review-verdict -->",
      "### Automated review: " + heading,
      "",
      "Commit `" + $sha + "` of pull request #" + $pr + " ([workflow run](" + $run_url + "))",
      ""
    ]
    + (if $status == "none" then
         ["No verdict was produced: " + $reason_text, "", "**This is not a pass.**"]
       else
         ["**Summary:** " + ($review.summary | code)]
         + (if $note == "" then [] else ["", $note] end)
         + ["", "**Findings (" + ($all | length | tostring) + ")**", ""]
         + (if ($all | length) == 0 then ["None."]
            else [ $all[:$shown] | to_entries[]
                   | "\(.key + 1). **\(.value.severity)** \(.value | location | code) \(.value.message | code)" ]
            end)
         + (if $shown < ($all | length)
            then ["", "_" + (($all | length) - $shown | tostring) + " more finding(s) not shown (comment length limit)._"]
            else [] end)
       end)
    + [
      "",
      "<sub>Produced from the diff alone by an automated reviewer. Text in code formatting is reviewer output shown as data. A pass is advisory: it does not replace CI or human review.</sub>"
    ])
  | join("\n");

(($review.findings // []) | length) as $n
| first(range($n; -1; -1) as $k | body($k) | select(length <= $max_chars)) // body(0)
