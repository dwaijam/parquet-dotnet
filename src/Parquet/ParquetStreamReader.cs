using Parquet.File;
using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using System.Collections;
using Parquet.Data.Predicates;
using System.Linq;
using Parquet.Thrift;

namespace Parquet
{
   /// <summary>
   /// Implements a lazy Apache Parquet format reader. 
   /// </summary>
   public class ParquetStreamReader : ParquetActor, IDisposable
   {
      private readonly Stream _input;
      private readonly Schema _schema;
      private readonly FileMetaData _meta;
      private readonly ThriftFooter _footer;
      private readonly ParquetOptions _formatOptions;

      /// <summary>
      /// Gets dataset schema
      /// </summary>
      public Schema Schema => _schema;

      /// <summary>
      /// Gets the total row count in the source file
      /// </summary>
      public long TotalRowCount { get; }

      /// <summary>
      /// Creates an instance from input stream
      /// </summary>
      /// <param name="input">Input stream, must be readable and seekable</param>
      /// <param name="formatOptions">Optional reader options</param>
      /// <exception cref="ArgumentNullException">input</exception>
      /// <exception cref="ArgumentException">stream must be readable and seekable - input</exception>
      /// <exception cref="IOException">not a Parquet file (size too small)</exception>
      public ParquetStreamReader(Stream input, ParquetOptions formatOptions = null) : base(input)
      {
         _input = input ?? throw new ArgumentNullException(nameof(input));
         if (!input.CanRead || !input.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(input));
         if (_input.Length <= 8) throw new IOException("not a Parquet file (size too small)");

         ValidateFile();
         _formatOptions = formatOptions ?? new ParquetOptions();
         _meta = ReadMetadata();
         _footer = new ThriftFooter(_meta);
         _schema = _footer.CreateModelSchema(_formatOptions);
         TotalRowCount = _meta.Num_rows;
      }

      /// <summary>
      /// Reads the file
      /// </summary>
      /// <param name="fullPath">The full path.</param>
      /// <param name="formatOptions">Optional reader options.</param>
      public ParquetStreamReader(string fullPath, ParquetOptions formatOptions = null) 
         : this(System.IO.File.OpenRead(fullPath), formatOptions)
      {
      }

      /// <summary>
      /// Test read, to be defined
      /// </summary>
      public DataSet Read(ReaderOptions readerOptions = null)
      {
         readerOptions = readerOptions ?? new ReaderOptions();
         readerOptions.Validate();
         FieldPredicate[] fieldPredicates = PredicateFactory.CreateFieldPredicates(readerOptions);
         var pathToValues = new Dictionary<string, IList>();
         long pos = 0;
         long rowsRead = 0;

         foreach (RowGroup rg in _meta.Row_groups)
         {
            //check whether to skip RG completely
            if ((readerOptions.Count != -1 && rowsRead >= readerOptions.Count) ||
               (readerOptions.Offset > pos + rg.Num_rows - 1))
            {
               pos += rg.Num_rows;
               continue;
            }

            long offset = Math.Max(0, readerOptions.Offset - pos);
            long count = readerOptions.Count == -1 ? rg.Num_rows : Math.Min(readerOptions.Count - rowsRead, rg.Num_rows);

            for (int icol = 0; icol < rg.Columns.Count; icol++)
            {
               Thrift.ColumnChunk cc = rg.Columns[icol];
               string path = cc.GetPath();
               if (fieldPredicates != null && !fieldPredicates.Any(p => p.IsMatch(cc, path))) continue;

               var columnarReader = new ColumnarReader(_input, cc, _footer, _formatOptions);

               try
               {
                  IList chunkValues = columnarReader.Read(offset, count);

                  if (!pathToValues.TryGetValue(path, out IList allValues))
                  {
                     pathToValues[path] = chunkValues;
                  }
                  else
                  {
                     foreach (object v in chunkValues)
                     {
                        allValues.Add(v);
                     }
                  }

                  if (icol == 0)
                  {
                     //todo: this may not work
                     rowsRead += chunkValues.Count;
                  }
               }
               catch (Exception ex)
               {
                  throw new ParquetException($"fatal error reading column '{path}'", ex);
               }
            }

            pos += rg.Num_rows;
         }
     
         var ds = new DataSet(_schema, pathToValues, _meta.Num_rows, _meta.Created_by);
         Dictionary<string, string> customMetadata = _footer.CustomMetadata;
         if (customMetadata != null) ds.Metadata.Custom.AddRange(customMetadata);
         ds.Thrift = _meta;
         return ds;
      }

      /// <summary>
      /// Disposes 
      /// </summary>
      public void Dispose()
      {
      }
   }
}