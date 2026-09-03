fn main() {
    if std::env::var_os("CARGO_FEATURE_TEST_UTIL").is_some()
        && std::env::var("PROFILE").as_deref() == Ok("release")
    {
        panic!("the utopia-llm test-util feature is forbidden in release builds");
    }
}
