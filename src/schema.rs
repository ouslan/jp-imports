// @generated automatically by Diesel CLI.

diesel::table! {
    hts (hts_id) {
        hts_id -> Int4,
        current_year -> Nullable<Int8>,
        #[max_length = 255]
        current_code -> Nullable<Varchar>,
        current_description -> Nullable<Text>,
        year -> Nullable<Int8>,
        #[max_length = 255]
        code -> Nullable<Varchar>,
    }
}

diesel::table! {
    naics (naics_id) {
        naics_id -> Int4,
        current_year -> Nullable<Int8>,
        #[max_length = 255]
        current_code -> Nullable<Varchar>,
        current_description -> Nullable<Text>,
        year -> Nullable<Int8>,
        #[max_length = 255]
        code -> Nullable<Varchar>,
    }
}

diesel::table! {
    products (product_id) {
        product_id -> Int4,
        hts_id -> Nullable<Int8>,
        naics_id -> Nullable<Int8>,
    }
}

diesel::table! {
    transactions (transaction_id) {
        transaction_id -> Int4,
        date -> Nullable<Date>,
        value -> Nullable<Int8>,
        product_id -> Nullable<Int8>,
    }
}

diesel::joinable!(products -> hts (hts_id));
diesel::joinable!(products -> naics (naics_id));
diesel::joinable!(transactions -> products (product_id));

diesel::allow_tables_to_appear_in_same_query!(
    hts,
    naics,
    products,
    transactions,
);
