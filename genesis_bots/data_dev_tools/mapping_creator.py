

# needs a place to keep track of the status of each mapping
# needs to be able to "iterate" over an existing mapping document

# on first run
# inputs:
#    name of mapping creation run 
#    query to the requirements set for the mapping
#    save them to a mapping_work table for this mapping run
#    baseline dbt template
#    schema info / ddls for source tables
#    knowledge and best practices statement

# first run does this
#    save the initial state of the dbt template
#    saves the reqirements to mapping_work as pendingrm 
#    get the first mapping row
#    try to add the mapping
#    either:
#        - adds a proposed mapping, and logs its work and thought in the mapping_work table and tags it as "mapping attempted"
#        - generates a question, notes what the question is, saves it in mapping_work along with what it discovered so far
#    return to the caller the dbt document with the first mapping done assuming it proposed one

# next run
# adds one more pending mapping, using the same process as above but starting with the output of the first call
# considers whether to add it to an existing CTE if any or a new CTE (bias to new CTE if in doubt)
#  - logs the same stuff
#  - generates a "DIFF" statement showing the additions it made / changes it made

# next run
# process 5 more pending mappings

# next run
# process all remaining ones that are still not attempte
# 

# then, once all are attempted
# saves the result to github (or database for now)
# slacks people for followup (along with a reference so when it gets answers it knows what its related to)
# creates jira ticket(s) for its questions and routes them to someone

# after that
# monitor for people answering questions on slack or jira
# monitor for new requirements
# as new info comes in, re attempt the mappings
# monitor for new mappings to be assigned to it that it needs to instantiate

# tables needed
# my_mappings
#     mapping_id, name of mapping, jira ticket that started it, overall status (pending, in progress, ready to test)
# mapping_rows
#     mapping_id, status of mapping, logic of mapping, thought its done about the mapping, escallation/question info, slack threads / jira tickets to track for followup
# mapping_results
#     mapping_id, current mapping doc


# create_mappings
# actions:  instantiate, attempt, status, results
# params: mapping_id, mapping_name, number_to_attempt_before_returning

# ideas :
# use difflib to have the bot propose DIFFS vs outputting the whole dbt
# use a local git repository to build up each step as DIFFs are applied
# use a git viewer to be able to see what it did

# OR, more generically 

# a more generic to do list
# to do list gets "add a file to hold the mappings, use this to start, file is here"
# then todo list gets the 113 requirments...
# .... add a column x to the mapping y, that does z...
# ... then bot wakes up, sees todo list, checks out one of the items (for the thread), works on it, takes any actions (commits a DIFF to a mapping, or escallates for help), updates the status
# ... has some TODOs that are dependent on other ones being done (all the mappings done before setting the mapping as done and passing to qa bot)
# then when someone responds to it, it can know what todo it relates to and work on it
# and it can preoidically bug people to help it when waiting, or escallate to additional people
# .. have it also diff its knowledge document in this manner when it learns something new... so humans can also see and comment or correct the knowledge

# needs
#  basic "make and diff doc" tools
#  gather semantic knowledge tool (search metadata, extended)
#  docs are stored in stage files (mounted so they can be diffed and git tracked easilly)
#  todo list and CRUD for it
#  a loop that says "work on something from your todo list"
#  a way to checkout a TODO for a period of time so another threads doesnt do something parallel with it
#  a knowledge document or library of them that it can make DIFF changes to, visible to people too
#  also a "daily/hourly actions" list that it does every day or hour (check for new jira tickets, create todos when new stuff is assigned/verify that stuff is still assigned that its working on)

# test with
# a basic short story opening
# todos to do 10 things to the story
# have it do those things making diffs
# have it wait for all the todos to complete
# finalize the result
# have some of them require outreach to someone to pick something for the story

