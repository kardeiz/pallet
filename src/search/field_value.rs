pub trait FieldValue: Clone {
    type FieldOptionsType;
    fn default_field_options() -> Self::FieldOptionsType;
    fn field_entry<I: Into<String>, T: Into<Self::FieldOptionsType>>(
        name: I,
        field_options: Option<T>,
    ) -> tantivy::schema::FieldEntry;
    fn into_value(self) -> Option<tantivy::schema::Value>;
}

impl FieldValue for String {
    type FieldOptionsType = tantivy::schema::TextOptions;

    fn default_field_options() -> Self::FieldOptionsType {
        tantivy::schema::TEXT
    }

    fn field_entry<I: Into<String>, T: Into<Self::FieldOptionsType>>(
        name: I,
        field_options: Option<T>,
    ) -> tantivy::schema::FieldEntry {
        tantivy::schema::FieldEntry::new_text(
            name.into(),
            field_options.map(Into::into).unwrap_or_else(Self::default_field_options),
        )
    }

    fn into_value(self) -> Option<tantivy::schema::Value> {
        Some(self.into())
    }
}

impl FieldValue for u64 {
    type FieldOptionsType = tantivy::schema::IntOptions;

    fn default_field_options() -> Self::FieldOptionsType {
        tantivy::schema::INDEXED.into()
    }

    fn field_entry<I: Into<String>, T: Into<Self::FieldOptionsType>>(
        name: I,
        field_options: Option<T>,
    ) -> tantivy::schema::FieldEntry {
        tantivy::schema::FieldEntry::new_u64(
            name.into(),
            field_options.map(Into::into).unwrap_or_else(Self::default_field_options),
        )
    }

    fn into_value(self) -> Option<tantivy::schema::Value> {
        Some(self.into())
    }
}

impl FieldValue for i64 {
    type FieldOptionsType = tantivy::schema::IntOptions;

    fn default_field_options() -> Self::FieldOptionsType {
        tantivy::schema::INDEXED.into()
    }

    fn field_entry<I: Into<String>, T: Into<Self::FieldOptionsType>>(
        name: I,
        field_options: Option<T>,
    ) -> tantivy::schema::FieldEntry {
        tantivy::schema::FieldEntry::new_i64(
            name.into(),
            field_options.map(Into::into).unwrap_or_else(Self::default_field_options),
        )
    }

    fn into_value(self) -> Option<tantivy::schema::Value> {
        Some(self.into())
    }
}

impl FieldValue for f64 {
    type FieldOptionsType = tantivy::schema::IntOptions;

    fn default_field_options() -> Self::FieldOptionsType {
        tantivy::schema::INDEXED.into()
    }

    fn field_entry<I: Into<String>, T: Into<Self::FieldOptionsType>>(
        name: I,
        field_options: Option<T>,
    ) -> tantivy::schema::FieldEntry {
        tantivy::schema::FieldEntry::new_f64(
            name.into(),
            field_options.map(Into::into).unwrap_or_else(Self::default_field_options),
        )
    }

    fn into_value(self) -> Option<tantivy::schema::Value> {
        Some(self.into())
    }
}

impl FieldValue for tantivy::DateTime {
    type FieldOptionsType = tantivy::schema::IntOptions;

    fn default_field_options() -> Self::FieldOptionsType {
        tantivy::schema::INDEXED.into()
    }

    fn field_entry<I: Into<String>, T: Into<Self::FieldOptionsType>>(
        name: I,
        field_options: Option<T>,
    ) -> tantivy::schema::FieldEntry {
        tantivy::schema::FieldEntry::new_date(
            name.into(),
            field_options.map(Into::into).unwrap_or_else(Self::default_field_options),
        )
    }

    fn into_value(self) -> Option<tantivy::schema::Value> {
        Some(self.into())
    }
}

impl<F: FieldValue> FieldValue for Option<F> {
    type FieldOptionsType = F::FieldOptionsType;
    fn default_field_options() -> Self::FieldOptionsType {
        F::default_field_options()
    }
    fn field_entry<I: Into<String>, T: Into<Self::FieldOptionsType>>(
        name: I,
        field_options: Option<T>,
    ) -> tantivy::schema::FieldEntry {
        F::field_entry(name, field_options)
    }

    fn into_value(self) -> Option<tantivy::schema::Value> {
        self.and_then(FieldValue::into_value)
    }
}
