#[macro_export]
macro_rules! strong_id {
    ($Outer:ident, $Inner:ty) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $Outer($Inner);
        impl std::fmt::Display for $Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
        impl From<$Inner> for $Outer {
            fn from(value: $Inner) -> Self {
                Self(value)
            }
        }
        impl From<$Outer> for $Inner {
            fn from(value: $Outer) -> Self {
                value.0
            }
        }
    };
}
