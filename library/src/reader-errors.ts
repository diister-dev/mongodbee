export class ReaderDefinitionError extends Error {
  override readonly name = "ReaderDefinitionError";
}

export class ReaderNotRegisteredError extends Error {
  override readonly name = "ReaderNotRegisteredError";
}

export class ReaderDatabaseError extends Error {
  override readonly name = "ReaderDatabaseError";
}

export class ReaderArgumentError extends Error {
  override readonly name = "ReaderArgumentError";
}
