use anchor_client::solana_sdk::message::CompileError;
use anchor_client::solana_sdk::signer::SignerError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ErrorKind {
    #[error("Solana rpc client error: {0:#?}")]
    SolanaRpcError(#[from] solana_client::client_error::ClientError),

    #[error(transparent)]
    SignerError(#[from] SignerError),

    #[error(transparent)]
    TransactionCompileError(#[from] CompileError),

    #[error("No instruction to include in the transaction")]
    NoInstructions,

    #[error("No transaction to send")]
    NoTransactions,

    #[error("Anchor error: {0:#?}")]
    AnchorError(Box<anchor_client::anchor_lang::prelude::AnchorError>),

    #[error("Anchor program error: {0:#?}")]
    AnchorProgramError(Box<anchor_client::anchor_lang::prelude::ProgramErrorWithOrigin>),

    #[error("Transaction error: {0:#?}")]
    TransactionError(#[from] anchor_client::solana_sdk::transaction::TransactionError),

    #[error("Error while deserializing an account: {0}")]
    DeserializationError(String),

    #[error("Error while getting the recommended fee: {0}")]
    SolanaCompassFetchError(#[from] reqwest::Error),

    #[error("Error trying to parse the recommended fees")]
    SolanaCompassReturnInvalid,
}

impl From<anchor_client::anchor_lang::error::Error> for ErrorKind {
    fn from(err: anchor_client::anchor_lang::error::Error) -> Self {
        use anchor_client::anchor_lang::error::Error as AnchorError;
        match err {
            AnchorError::AnchorError(e) => ErrorKind::AnchorError(e.into()),
            AnchorError::ProgramError(e) => ErrorKind::AnchorProgramError(e.into()),
        }
    }
}
