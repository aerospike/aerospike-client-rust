#[cfg(all(feature = "rt-tokio", feature = "rt-async-std"))]
compile_error!("proptest_async: multiple async executors are selected");

#[cfg(feature = "rt-tokio")]
#[allow(dead_code)]
#[inline]
pub fn block_on<F>(f: F) -> F::Output
where
    F: core::future::Future,
{
    let _ = env_logger::try_init();
    let rt = &crate::common::RUNTIME;
    // let rt = tokio::runtime::Builder::new_current_thread()
    //     .enable_time()
    //     .build()
    //     .unwrap();

    rt.block_on(f)
}

#[cfg(feature = "rt-async-std")]
#[allow(dead_code)]
#[inline]
pub fn block_on<F>(f: F) -> F::Output
where
    F: core::future::Future,
{
    let _ = env_logger::try_init();
    aerospike_rt::task::block_on(f)
}

/// Adjustment of proptest's macro to enable async tests via async_std.
#[cfg(test)]
#[macro_export]
macro_rules! proptest {
    (#![proptest_config($config:expr)]
     $(
        $(#[$meta:meta])*
        fn $test_name:ident($($parm:pat in $strategy:expr),+ $(,)?) $body:block
    )*) => {
        $(
            $(#[$meta])*
            fn $test_name() {
                let mut config = ::proptest::test_runner::contextualize_config($config.clone());
                config.test_name = Some(
                    concat!(module_path!(), "::", stringify!($test_name)));
                ::proptest::proptest_helper!(@_BODY config ($($parm in $strategy),+) [] $body);
            }
        )*
    };
    (#![proptest_config($config:expr)]
     $(
        $(#[$meta:meta])*
        fn $test_name:ident($($arg:tt)+) $body:block
    )*) => {
        $(
            $(#[$meta])*
            fn $test_name() {
                let mut config = ::proptest::test_runner::contextualize_config($config.clone());
                config.test_name = Some(
                    concat!(module_path!(), "::", stringify!($test_name)));
                ::proptest::proptest_helper!(@_BODY2 config ($($arg)+) [] $body);
            }
        )*
    };

    ($(
        $(#[$meta:meta])*
        fn $test_name:ident($($parm:pat in $strategy:expr),+ $(,)?) $body:block
    )*) => { $crate::proptest! {
        #![proptest_config(::proptest::test_runner::Config::default())]
        $($(#[$meta])*
          fn $test_name($($parm in $strategy),+) $body)*
    } };

    ($(
        $(#[$meta:meta])*
        fn $test_name:ident($($arg:tt)+) $body:block
    )*) => { $crate::proptest! {
        #![proptest_config(::proptest::test_runner::Config::default())]
        $($(#[$meta])*
        fn $test_name($($arg)+) $body)*
    } };



    (#![proptest_config($config:expr)]
     $(
        $(#[$meta:meta])*
       async fn $test_name:ident($($parm:pat in $strategy:expr),+ $(,)?) $body:block
    )*) => {
        $(
            $(#[$meta])*
            fn $test_name() {
                let mut config = ::proptest::test_runner::contextualize_config($config.clone());
                config.test_name = Some(
                    concat!(module_path!(), "::", stringify!($test_name)));
                ::proptest::proptest_helper!(@_BODY config ($($parm in $strategy),+) [] $crate::proptest_async::block_on(async move $body));
            }
        )*
    };

    (#![proptest_config($config:expr)]
     $(
        $(#[$meta:meta])*
        async fn $test_name:ident($($arg:tt)+) $body:block
    )*) => {
        $(
            $(#[$meta])*
            fn $test_name() {
                let mut config = ::proptest::test_runner::contextualize_config($config.clone());
                config.test_name = Some(
                    concat!(module_path!(), "::", stringify!($test_name)));
                ::proptest::proptest_helper!(@_BODY2 config ($($arg)+) [] $crate::proptest_async::block_on(async move $body));
            }
        )*
    };

    ($(
        $(#[$meta:meta])*
        async fn $test_name:ident($($parm:pat in $strategy:expr),+ $(,)?) $body:block
    )*) => { $crate::proptest! {
        #![proptest_config(::proptest::test_runner::Config::default())]
        $($(#[$meta])*
        async fn $test_name($($parm in $strategy),+) $body)*
    } };

    ($(
        $(#[$meta:meta])*
        async fn $test_name:ident($($arg:tt)+) $body:block
    )*) => { $crate::proptest! {
        #![proptest_config(::proptest::test_runner::Config::default())]
        $($(#[$meta])*
        async fn $test_name($($arg)+) $body)*
    } };

    (|($($parm:pat in $strategy:expr),+ $(,)?)| $body:expr) => {
        $crate::proptest!(
            proptest::test_runner::Config::default(),
            |($($parm in $strategy),+)| $body)
    };

    (move |($($parm:pat in $strategy:expr),+ $(,)?)| $body:expr) => {
        $crate::proptest!(
            ::proptest::test_runner::Config::default(),
            move |($($parm in $strategy),+)| $body)
    };

    (|($($arg:tt)+)| $body:expr) => {
        $crate::proptest!(
            ::proptest::test_runner::Config::default(),
            |($($arg)+)| $body)
    };

    (move |($($arg:tt)+)| $body:expr) => {
        $crate::proptest!(
            ::proptest::test_runner::Config::default(),
            move |($($arg)+)| $body)
    };

    ($config:expr, |($($parm:pat in $strategy:expr),+ $(,)?)| $body:expr) => { {
        let mut config = ::proptest::test_runner::contextualize_config($config.__sugar_to_owned());
        ::proptest::sugar::force_no_fork(&mut config);
        ::proptest::proptest_helper!(@_BODY config ($($parm in $strategy),+) [] $body)
    } };

    ($config:expr, move |($($parm:pat in $strategy:expr),+ $(,)?)| $body:expr) => { {
        let mut config = ::proptest::test_runner::contextualize_config($config.__sugar_to_owned());
        ::proptest::sugar::force_no_fork(&mut config);
        ::proptest::proptest_helper!(@_BODY config ($($parm in $strategy),+) [move] $body)
    } };

    ($config:expr, |($($arg:tt)+)| $body:expr) => { {
        let mut config = ::proptest::test_runner::contextualize_config($config.__sugar_to_owned());
        ::proptest::sugar::force_no_fork(&mut config);
        ::proptest::proptest_helper!(@_BODY2 config ($($arg)+) [] $body);
    } };

    ($config:expr, move |($($arg:tt)+)| $body:expr) => { {
        let mut config = $crate::test_runner::contextualize_config($config.__sugar_to_owned());
        ::proptest::sugar::force_no_fork(&mut config);
        ::proptest::proptest_helper!(@_BODY2 config ($($arg)+) [move] $body);
    } };
}

// make the macro available in other modules
pub(crate) use proptest;
