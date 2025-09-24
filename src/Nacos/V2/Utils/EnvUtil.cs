namespace Nacos.V2.Utils
{
    using System;

    public static class EnvUtil
    {
        public static string GetEnvValue(string envName) =>
            Environment.GetEnvironmentVariable(envName) ??
            Environment.GetEnvironmentVariable(envName.Replace('.', '_'));

        public static string GetEnvValue(string envName, string defaultValue)
        {
            var value = GetEnvValue(envName);

            return value.IsNullOrWhiteSpace() ? defaultValue : value;
        }
    }
}
