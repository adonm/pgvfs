#pragma once

#include "duckdb.hpp"

namespace duckdb {

class PgvfsExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

//! Tantivy search splits on any DuckDB filesystem (tantivy_functions.cpp).
void RegisterTantivy(ExtensionLoader &loader);
void ForgetTantivyPrefix(ClientContext &context, const string &prefix);

} // namespace duckdb
