/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.s3.decoder;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;

import java.util.Map;
import java.util.Optional;

/**
 * Implementations decode a row from bytes and add field value providers for all decodable columns.
 */
public interface RowDecoder
{
    /**
     * Decodes a given set of bytes into field values.
     *
     * @param data The row data to decode.
     * @param dataMap The row data as fields map
     * @return Returns mapping from column handle to decoded value. Unmapped columns will be reported as null. Optional.empty() signals decoding error.
     */
    Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(
            byte[] data,
            Map<String, String> dataMap);

    /**
     * Decodes a given set of bytes into field values.
     *
     * @param data The row data to decode.
     * @param offset The row data offset to decode.
     * @param length The row data length to decode.
     * @param dataMap The row data as fields map
     * @return Returns mapping from column handle to decoded value. Unmapped columns will be reported as null. Optional.empty() signals decoding error.
     */
    Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(
            byte[] data,
            int offset,
            int length,
            Map<String, String> dataMap);
}
