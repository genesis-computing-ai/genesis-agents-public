from   bs4                      import BeautifulSoup
import collections.abc
from   genesis_bots.core.bot_os_artifacts \
                                import (ARTIFACT_ID_REGEX, get_artifacts_store,
                                        lookup_artifact_markdown)
from   genesis_bots.core.logging_config \
                                import logger
import re
from   urllib.parse             import urlencode, urlunparse


from   genesis_bots.core.bot_os_tools2 \
                                import (BOT_ID_IMPLICIT_FROM_CONTEXT,
                                        THREAD_ID_IMPLICIT_FROM_CONTEXT,
                                        ToolFuncGroup, gc_tool)

from   genesis_bots.connectors  import get_global_db_connector
db_adapter = get_global_db_connector()

from   .tool_helpers            import get_sys_email

send_email_tools = ToolFuncGroup(
    name="send_email_tools",
    description="Sends an email using Snowflake's SYSTEM$SEND_EMAIL function.",
    lifetime="PERSISTENT",
)

# We use this URL to include the genesis logo in snowflake-generated emails.
# TODO: use a permanent URL under the genesiscomputing.ai domain
GENESIS_LOGO_URL = "https://i0.wp.com/genesiscomputing.ai/wp-content/uploads/2024/05/Genesis-Computing-Logo-White.png"


@gc_tool(
    to_addr_list="A comman-separated list of recipient email addresses.",
    subject="The subject of the email.",
    body=("The body content of the email. "
          "When using mime_type='text/plain' you CAN use Slack-compatible markdown syntax. "
          "When using mime_type='text/html' DO NOT use markdown - use appropriate HTML tags and HTML formatting instead. "),
    purpose="A short description of the purpose of this email. This is stored as metadata for this email.",
    mime_type="The MIME type of the email body. Accepts 'text/plain' or 'text/html'. Defaults to 'text/html'.",
    include_genesis_logo="Whether or not to include the Genesis logo in the email body",
    save_as_artifact="Whether or not to save the output email as an artifact. When saved as an artifact, the email content and metadata can be retrieved later for future reference.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[send_email_tools],
)
def send_email(
    to_addr_list: str,
    subject: str,
    body: str,
    bot_id: str,
    thread_id: str = None,
    purpose: str = None,
    mime_type: str = "text/html",
    include_genesis_logo: bool = False,
    save_as_artifact: bool = True,
):
    """
    Sends an email using Snowflake's SYSTEM$SEND_EMAIL function.
    Both text/plain or text/html formats are supported. Prefer to always use text/html whenever possible, unless explicitly requested to use text/plain or encountering HTML formatting issues.

    IMPORTANT: Attachments are NOT SUPPORTED - do not use CID references in your email body.    
               To embed a previously generated artifact (such as a plot, code snippet, etc) in your email,
               embed those artifacts using artifact markdown notation `[{description}][artifact:/{artifact_id}]`. 
               Those link notations will be converted to externally-accessible URLs before the email is sent.

    Returns:
        dict: Result of the email sending operation.
    """

    #
    # -- Interactive testing example: --
    # Prompt: Use snowpark python to genreate a plot of the sine wave over the range 0 to 4*pi radians. Send the plot to aviv.dekel@genesiscomputing.ai
    # Expected result:
    # 1. A plot is generated and saved as an artifact
    # 2. The email is sent to aviv.dekel@genesiscomputing.ai
    # 3. The email body contains a Snowflake-signed URL (allowing the browser to download the artifact file from the stage in Snowflake)
    # 4. The email body also contains a footer 'Click here to explore more about <this email>' a link that re-opens a sessions with the Streamlit app chat page with the bot that generated the email, loading the eamil as an artifact.
    #
    #
    logger.info(f"Entering send_email with bot_id={bot_id}\nthread_id={thread_id}\nto_addr_list={to_addr_list}\nsubject={subject}\nbody={body}\npurpose={purpose}\nmime_type={mime_type}\ninclude_genesis_logo={include_genesis_logo}\nsave_as_artifact={save_as_artifact}")
    art_store = get_artifacts_store(db_adapter)  # used by helper functions below

    def _sanity_check_body(txt):
        # Check for HTML tags with 'href' or 'src' attributes using CID
        cid_pattern = re.compile(
            r'<[^>]+(?:href|src)\s*=\s*["\']cid:[^"\']+["\']', re.IGNORECASE
        )
        if cid_pattern.search(txt):
            raise ValueError(
                "The email body contains HTML tags with links or 'src' attributes using CID. Attachements are NOT SUPPORTED. "
                "Use artifact markdown notation to embed previously generated artifacts instead."
            )

        # Identify all markdowns and check for strictly formatted artifact markdowns
        markdown_pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
        matches = markdown_pattern.findall(txt)
        for description, url in matches:
            if url.startswith("artifact:"):
                artifact_pattern = re.compile(r"artifact:/(" + ARTIFACT_ID_REGEX + r")")
                if not artifact_pattern.match(url):
                    raise ValueError(
                        f"Improperly formatted artifact markdown detected: [{description}]({url})"
                    )

    def _strip_url_markdown(txt):
        # a helper function to strip any URL markdown and leave only the URL part
        # This is used in plain text mode, since we assume that the email rendering does not support markdown.
        pattern = r"(!?\[([^\]]+)\]\(((http[s]?|file|sandbox|artifact):/+[^\)]+)\))"  # regex for strings of the form '[description](url)' and '![description](url)'
        matches = re.findall(pattern, txt)
        for match in matches:
            txt = txt.replace(
                match[0], match[2]
            )  # Replace the entire match with just the URL part, omitting description
        return txt

    def construct_artifact_linkback(artifact_id, link_text=None):
        """
        Constructs a linkback URL for a given artifact ID. This URL should drop the user into the streamlit app
        with a new chat that brings up the context of this artifact for futher exploration.

        Args:
            artifact_id (str): The unique identifier of the artifact.
            link_text (str): the text to display for the artifact link. Defaults to the artifact's 'title_filename'

        Returns:
            a string to use as the link (in text or HTML, depending on mime_type)
        """
        # fetch the metadata
        dbtr = db_adapter
        try:
            metadata = art_store.get_artifact_metadata(artifact_id)
        except Exception as e:
            logger.error(f"Failed to get artifact metadata for {artifact_id}: {e}")
            return None

        # Resolve the bot_name from bot_id. We need this for the linkback URL since the streamlit app
        # manages the bots by name, not by id.
        # TODO: fix this as part of issue #89
        proj, schema = dbtr.genbot_internal_project_and_schema.split(".")
        bot_config = dbtr.db_get_bot_details(proj, schema, dbtr.bot_servicing_table_name, bot_id)

        # Construct linkback URL
        if dbtr.is_using_local_runner:
            app_ingress_base_url = "localhost:8501/"  # TODO: avoid hard-coding port (but could not find an avail config to pick this up from)
        else:
            app_ingress_base_url = dbtr.db_get_endpoint_ingress_url("streamlit")
        if not app_ingress_base_url:
            return None
        params = dict(
            bot_name=bot_config["bot_name"],
            action="show_artifact_context",  # IMPORTANT: keep this action name in sync with the action handling logic in the app.
            artifact_id=artifact_id,
        )
        linkback_url = urlunparse(
            (
                "http",  # scheme
                app_ingress_base_url,  # netloc (host)
                "",  # path
                "",  # params
                urlencode(params),  # query
                "",  # fragment
            )
        )
        link_text = link_text or metadata["title_filename"]
        if mime_type == "text/plain":
            return f"{link_text}: {linkback_url}"
        if mime_type == "text/html":
            return f"<a href='{linkback_url}'>{link_text}</a>"
        assert False  # unreachable

    def _handle_artifacts_markdown(txt) -> str:
        # a helper function that locates artifact references in the text (pseudo URLs that look like [description][artifact:/uuid] or ![description][artifact:/uuid])
        # and replaces those with an external URL (Snowflake-signed externalized URL)
        # returns the modified text, along with the artifact_ids that were extraced from the text.
        artifact_ids = []
        for markdown, description, artifact_id in lookup_artifact_markdown(
            txt, strict=False
        ):
            try:
                external_url = art_store.get_signed_url_for_artifact(artifact_id)
            except Exception as e:
                # if we failed, leave this URL as-is. It will likely be a broken URL but in an obvious way.
                pass
            else:
                if mime_type == "text/plain":
                    link = external_url
                elif mime_type == "text/html":
                    ameta = art_store.get_artifact_metadata(artifact_id)
                    title = ameta["title_filename"]
                    sanitized_title = re.sub(r"[^a-zA-Z0-9_\-:.]", "-", title)
                    amime = ameta["mime_type"]
                    if amime.startswith("image/"):
                        link = f'<img src="{external_url}" alt="{sanitized_title}" >'
                    elif amime.startswith("text/"):
                        link = f'<iframe src="{external_url}" frameborder="1">{title}</iframe>'
                    else:
                        link = f'<a href="{external_url}" download="{sanitized_title}">Download {title}</a>'
                else:
                    assert False  # unreachable
                txt = txt.replace(markdown, link)
            artifact_ids.append(artifact_id)
        return txt, artifact_ids

    def _externalize_raw_artifact_urls(txt):
        # a helper function that locates 'raw' artifact references in the text (pseudo URLs that look like artifact:/<uuid>)
        # and replaces those an external URL (Snowflake-signed externalized URL)
        # returns the modified text, along with the artifact_ids that were extraced from the text.
        # This is used to catch artifact references that were used outside of 'proper' artifact markdowns, which should have been
        # handled by _handle_artifacts_markdown.
        pattern = r"(?<=\W)(artifact:/(" + ARTIFACT_ID_REGEX + r"))(?=\W)"
        matches = re.findall(pattern, txt)

        artifact_ids = []
        if matches:
            for full_match, uuid in matches:
                try:
                    external_url = art_store.get_signed_url_for_artifact(uuid)
                except Exception as e:
                    # if we failed, leave this URL as-is. It will likely be a broken URL but in an obvious way.
                    logger.info(
                        f"ERROR externalizing URL for artifact {uuid} in email. Leaving as-is. Error = {e}"
                    )
                else:
                    txt = txt.replace(full_match, external_url)
                artifact_ids.append(uuid)
        return txt, artifact_ids

    def _save_email_as_artifact(
        art_subject, art_body, art_receipient, embedded_artifact_ids
    ):
        # Save the email body, along with useful medatada, as an artifact

        # Build the metadata for this artifact
        metadata = dict(
            mime_type=mime_type,
            thread_id=thread_id,
            bot_id=bot_id,
            title_filename=art_subject,
            func_name="send_email",
            thread_context=purpose,
            email_subject=art_subject,
            recipients=art_receipient,
            embedded_artifact_ids=list(embedded_artifact_ids),
        )
        # Create artifact
        suffix = ".html" if mime_type == "text/html" else ".txt"
        aid = art_store.create_artifact_from_content(
            art_body, metadata, content_filename=(subject + suffix)
        )
        return aid

    # Validate mime_type
    if mime_type not in ["text/plain", "text/html"]:
        raise ValueError(
            f"mime_type must be either 'text/plain' or 'text/html', got {mime_type}"
        )

    # Check if to_addr_list is a string representation of a list
    addresses = []  # Create a new list to store addresses
    if isinstance(to_addr_list, str):
        try:
            # Attempt to parse the string as a Python list
            if to_addr_list.startswith("[") and to_addr_list.endswith("]"):
                # Remove brackets and split by comma
                content = to_addr_list[1:-1]
                addresses = [
                    addr.strip().strip("'\"")
                    for addr in content.split(",")
                    if addr.strip()
                ]
                if not addresses:
                    raise ValueError(
                        "Failed to extract valid email addresses from the provided address list string ."
                    )
            else:
                # If it's not in list format, split by comma
                addresses = [
                    addr.strip() for addr in to_addr_list.split(",") if addr.strip()
                ]
        except Exception:
            # If parsing fails, split by comma
            addresses = [addr.strip() for addr in to_addr_list.split(",")]
    else:
        # If it's already a list-like object, convert to list
        addresses = list(to_addr_list)

    # Remove any empty strings and strip quotes from each address
    addresses = [addr.strip("'\"") for addr in addresses if addr]

    if not addresses:
        return {"Success": False, "Error": "No valid email addresses provided."}

    # Replace SYS$DEFAULT_EMAIL with the actual system default email
    addresses = [
        get_sys_email() if addr == "SYS$DEFAULT_EMAIL" else addr
        for addr in addresses
    ]

    # Join the email addresses with commas
    to_addr_string = ", ".join(addresses)

    # Build an 'origin line' to make it clear where this message is coming from. Prepend to body below
    # NOTE: conisder making this a footer?
    origin_line = f"🤖 This is an automated message from the Genesis Computing Native Application."
    if bot_id is not None:
        origin_line += f" Bot: {bot_id}."
    origin_line += "🤖\n\n"

    # Cleanup the orirignal body and save as artifact if requested
    body = body.replace("\\n", "\n")
    orig_body = body  # save for later/debugging

    # Sanity check the body for unsupported features and bad formatting
    try:
        _sanity_check_body(body)
    except ValueError as e:
        return {"Success": False, "Error": str(e)}

    # Handle artifact refs in the body - replace with external links
    body, embedded_artifact_ids = _handle_artifacts_markdown(body)
    body, more_embedded_artifact_ids = _externalize_raw_artifact_urls(body)
    embedded_artifact_ids.extend(more_embedded_artifact_ids)

    email_aid = None
    if save_as_artifact:
        email_aid = _save_email_as_artifact(
            subject, body, to_addr_string, embedded_artifact_ids
        )

    # build the artifact 'linkback' URLs footer
    if save_as_artifact:
        # When saving the email itself as an artifcat, do not include embedded artifacts
        linkbacks = [construct_artifact_linkback(email_aid, link_text="this email")]
    else:
        linkbacks = [construct_artifact_linkback(aid) for aid in embedded_artifact_ids]
        linkbacks = [
            link for link in linkbacks if link is not None
        ]  # remove any failures (best effort)

    # Force the body to HTML if the mime_type is text/html. Prepend origin line. externalize artifact links.
    if mime_type == "text/html":
        soup = BeautifulSoup(body, "html.parser")
        html_body = str(soup)

        # Check if the string already contains <html> and <body> tags
        if soup.body is None:
            html_body = f"<body>{html_body}</body>"

        if soup.html is None:
            html_body = f"<html>{html_body}</html>"
        soup = BeautifulSoup(html_body, "html.parser")

        assert soup.body is not None

        # Insert the origin message at the beginning of the body
        origin_elem = soup.new_tag("p")
        origin_elem.string = origin_line
        soup.body.insert(0, origin_elem)

        # Insert the Genesis logo at the top if include_genesis_logo is True
        if include_genesis_logo:
            link_tag = soup.new_tag("a", href="https://genesiscomputing.ai/")
            logo_tag = soup.new_tag(
                "img",
                src=GENESIS_LOGO_URL,
                style="margin-right:10px; height:50px;",
                alt="Genesis Computing",
            )
            link_tag.insert(0, logo_tag)
            # Ensure the logo is on its own line
            logo_container = soup.new_tag(
                "div", style="text-align:left; margin-bottom:1px;"
            )
            logo_container.insert(0, link_tag)
            soup.body.insert(0, logo_container)

        # Insert linkback URLs at the bottom
        if linkbacks:
            footer_elem = soup.new_tag("p")
            footer_elem.string = "Click here to explore more about "
            for link in linkbacks:
                footer_elem.append(BeautifulSoup(link, "html.parser"))
                footer_elem.append(" ")  # Add space between links
            soup.body.append(footer_elem)

        body = str(soup)

    elif mime_type == "text/plain":
        # For plain text, strip URL markdowns, and prepend the bot message
        body = _strip_url_markdown(body)
        body = origin_line + body
        # append linkbacks
        if linkbacks:
            body += "\n\n'Click here to explore more: '" + ", ".join(linkbacks)

    else:
        assert False, "Unreachable code"

    # Remove any instances of $$ from to_addr_string, subject and body
    # Fix double-backslashed unicode escape sequences in the body
    to_addr_string = to_addr_string.replace("$$", "")

    def unescape_unicode(match):
        return chr(int(match.group(1), 16))

    body = re.sub(r"\\u([0-9a-fA-F]{4})", unescape_unicode, body)
    if len(subject) == 0:
        subject = "Email from Genesis Bot"
        if bot_id is not None:
            subject += f" {bot_id}."
    subject = re.sub(r"\\u([0-9a-fA-F]{4})", unescape_unicode, subject)
    subject = subject.replace("$$", "")
    body = body.replace("$$", "")

    # Send the email
    query = f"""
        CALL SYSTEM$SEND_EMAIL(
            'genesis_email_int',
            $${to_addr_string}$$,
            $${subject}$$,
            $${body}$$,
            $${mime_type}$$
        );
        """

    # Execute the query using the database adapter's run_query method

    query_failed = False
    query_result = db_adapter.run_query(query, thread_id=thread_id, bot_id=bot_id)
    if isinstance(query_result, dict) and 'Error' in query_result:
        query_result = query_result['Error']
        query_failed = True
    elif isinstance(query_result, (list, tuple)) and len(query_result) > 0:
        query_result = query_result[0]

    if isinstance(query_result, collections.abc.Mapping) and not query_result.get(   "SYSTEM$SEND_EMAIL" ):
        # send failed. Delete the email artifact (if created) as it's useless.
        if email_aid:
            art_store.delete_artifacts([email_aid])
        result = query_result
    elif query_failed:
        result = {"Success": False, "Error": query_result}
        if 'not yet validated' in query_result:
            result["Suggestion"] = "Suggest to the user they may need to set up and balidate their email address on their Snowflake account, by following the instructions at https://docs.snowflake.com/en/user-guide/ui-snowsight-profile#verify-your-email-address"
        if "Integration 'GENESIS_EMAIL_INT' does not exist" in query_result:
            result["Suggestion"] = "Suggest to the user that the Genesis admin need to configure the 'genesis_email_int' integration on their Snowflake account, by following the instructions in the Genesis Streamlit GUI or Documentation"
    else:
        assert (
            len(query_result) == 1
        )  # we expect a successful SYSTEM$SEND_EMAIL to contain a single line resultset
        result = {"Success": True}
        if email_aid:
            result["Suggestion"] = (
                f"This email was saved as an artifact with artifact_id={email_aid}. "
                "Suggest to the user to refer to this email in the future from any session using this artifact identifier."
            )

    assert result
    return result

_send_email_functions = [send_email,]

# Called from bot_os_tools.py to update the global list of functions
def get_send_email_functions():
    return _send_email_functions
