
# Run make_alpha_sis_launch.py
python3 ./streamlit_gui/make_alpha_sis_launch.py


# Upload streamlit files
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/Genesis.py @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/utils.py @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/*.png @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/*.yml @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit files
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/.streamlit/config.toml @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit/.streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Upload streamlit page files
snow sql -c GENESIS-ALPHA-CONSUMER -q "PUT file:///Users/justin/Documents/Code/genesis/streamlit_gui/page_files/*.py @genesis_test.genesis_jl.stream_test/code_artifacts/streamlit/page_files AUTO_COMPRESS=FALSE OVERWRITE=TRUE"


