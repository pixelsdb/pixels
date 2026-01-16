python gen_complex_data.py
../../build/release/extension/pixels/pixels-cli/pixels-cli << EOF
LOAD -o /home/whz/test/pixels/cpp/testcase/pixels-cli-tests/test_origin -t /home/whz/test/pixels/cpp/testcase/pixels-cli-tests/test_target -s /home/whz/test/pixels/cpp/testcase/pixels-cli-tests/complex_test.schema -n 1000 -r ,
EOF