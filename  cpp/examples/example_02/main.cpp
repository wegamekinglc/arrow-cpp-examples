//
// Created by wegam on 2023/5/15.
//

#include <random>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <iostream>

int main(int argc, char** argv) {
    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::normal_distribution<> d{5, 2};

    auto pool = arrow::default_memory_pool();
    arrow::DoubleBuilder builder{arrow::float64(), pool};

    constexpr auto ncols = 16;
    constexpr auto nrows = 8192;
    arrow::ArrayVector columns(ncols);
    arrow::FieldVector fields;
    for (int i = 0; i < ncols; ++i) {
        for (int j = 0; j < nrows; ++j) {
            auto status = builder.Append(d(gen));
        }
        auto status = builder.Finish(&columns[i]);
        if (!status.ok()) {
            std::cerr << status.message() << std::endl;
            // handle the error
        }
        fields.push_back(arrow::field("c" + std::to_string(i),
                                      arrow::float64()));
    }
    auto rb = arrow::RecordBatch::Make(arrow::schema(fields),
                                       columns[0]->length(), columns);
    std::cout << rb->ToString() << std::endl;
}