using Xunit;
using Moq;
using FluentAssertions;
using ProductDataIngestion.Services;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories;

namespace ProductDataIngestion.Tests.Services
{
    /// <summary>
    /// IngestService のユニットテスト
    /// </summary>
    public class IngestServiceTests
    {
        private readonly Mock<IBatchRepository> _mockBatchRepo;
        private readonly Mock<IProductRepository> _mockProductRepo;
        private readonly string _testConnectionString;

        public IngestServiceTests()
        {
            _mockBatchRepo = new Mock<IBatchRepository>();
            _mockProductRepo = new Mock<IProductRepository>();
            _testConnectionString = "Host=localhost;Port=25432;Database=test_db;Username=test;Password=test";
        }

        #region ConvertToPascalCase テスト

        [Theory]
        [InlineData("product_cd", "ProductCd")]
        [InlineData("brand_id", "BrandId")]
        [InlineData("category_1_id", "Category1Id")]
        [InlineData("", "")]
        [InlineData("simple", "Simple")]
        public void ConvertToPascalCase_正しく変換できる(string input, string expected)
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ConvertToPascalCase",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { input }) as string;

            // Assert
            result.Should().Be(expected);
        }

        #endregion

        #region ApplyTransformExpression テスト

        [Theory]
        [InlineData("  test  ", "trim(@)", "test")]
        [InlineData("hello", "upper(@)", "HELLO")]
        [InlineData("", "nullif(@,'')", null)]
        [InlineData("   ", "nullif(@,'')", null)]
        [InlineData("  Hello  ", "trim(@),upper(@)", "HELLO")]
        public void ApplyTransformExpression_正しく変換できる(string input, string transformExpr, string expected)
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ApplyTransformExpression",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { input, transformExpr }) as string;

            // Assert
            result.Should().Be(expected);
        }

        [Fact]
        public void ApplyTransformExpression_Nullを渡すとNullを返す()
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ApplyTransformExpression",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object?[] { null, "trim(@)" }) as string;

            // Assert
            result.Should().BeNull();
        }

        [Fact]
        public void ApplyTransformExpression_空のTransformExprはデフォルトTrimを適用()
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ApplyTransformExpression",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { "  test　", "" }) as string;

            // Assert
            result.Should().Be("test"); // 半角・全角スペース削除
        }

        #endregion

        #region ConvertPostgreSqlFormatToDotNet テスト

        [Theory]
        [InlineData("YYYY-MM-DD", "yyyy-MM-dd")]
        [InlineData("YYYY/MM/DD", "yyyy/MM/dd")]
        [InlineData("DD-MM-YYYY", "dd-MM-yyyy")]
        [InlineData("YYYY年MM月DD日", "yyyy年MM月dd日")]
        public void ConvertPostgreSqlFormatToDotNet_正しく変換できる(string pgFormat, string expected)
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ConvertPostgreSqlFormatToDotNet",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { pgFormat }) as string;

            // Assert
            result.Should().Be(expected);
        }

        #endregion

        #region ParseDateExpression テスト

        [Theory]
        [InlineData("2025-10-22", "to_timestamp(@,'YYYY-MM-DD')", "2025-10-22")]
        [InlineData("22/10/2025", "to_timestamp(@,'DD/MM/YYYY')", "2025-10-22")]
        [InlineData("2025年10月22日", "to_timestamp(@,'YYYY年MM月DD日')", "2025-10-22")]
        public void ParseDateExpression_正しく日付変換できる(string value, string expression, string expected)
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ParseDateExpression",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { value, expression }) as string;

            // Assert
            result.Should().Be(expected);
        }

        [Fact]
        public void ParseDateExpression_無効な日付は元の値を返す()
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ParseDateExpression",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { "invalid-date", "to_timestamp(@,'YYYY-MM-DD')" }) as string;

            // Assert
            result.Should().Be("invalid-date");
        }

        [Fact]
        public void ParseDateExpression_不正なフォーマットは元の値を返す()
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ParseDateExpression",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { "2025-10-22", "invalid_expression" }) as string;

            // Assert
            result.Should().Be("2025-10-22");
        }

        #endregion

        #region GetEncodingFromCharacterCode テスト

        [Theory]
        [InlineData("UTF-8", "UTF-8")]
        [InlineData("SHIFT_JIS", "Shift_JIS")]
        [InlineData("EUC-JP", "EUC-JP")]
        [InlineData("UNKNOWN", "UTF-8")] // デフォルトはUTF-8
        public void GetEncodingFromCharacterCode_正しいエンコーディングを返す(string characterCd, string expectedEncodingName)
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("GetEncodingFromCharacterCode",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            var result = method?.Invoke(service, new object[] { characterCd }) as System.Text.Encoding;

            // Assert
            result.Should().NotBeNull();
            result!.WebName.Should().Be(expectedEncodingName.ToLowerInvariant());
        }

        #endregion

        #region ValidateCompanyCodeAsync テスト

        [Fact]
        public async Task ValidateCompanyCodeAsync_空のコードで例外を投げる()
        {
            // Arrange
            var service = new IngestService(_testConnectionString, _mockBatchRepo.Object, _mockProductRepo.Object);
            var method = typeof(IngestService).GetMethod("ValidateCompanyCodeAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<System.Reflection.TargetInvocationException>(async () =>
            {
                var task = method?.Invoke(service, new object[] { "" }) as Task;
                await task!;
            });

            exception.InnerException.Should().BeOfType<IngestException>();
            var ingestEx = exception.InnerException as IngestException;
            ingestEx!.ErrorCode.Should().Be(ErrorCodes.MISSING_COLUMN);
        }

        #endregion
    }
}
