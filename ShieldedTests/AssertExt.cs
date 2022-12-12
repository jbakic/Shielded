namespace ShieldedTests
{
    public static class AssertExt
    {
        public static void AreEqual(int expected, int actual)
        {
            Assert.That(actual, Is.EqualTo(expected));
        }

        public static void AreNotEqual(int expected, int actual)
        {
            Assert.That(actual, Is.Not.EqualTo(expected));
        }
    }
}
