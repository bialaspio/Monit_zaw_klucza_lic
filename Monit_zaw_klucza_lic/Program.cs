using System;
using System.Collections.Generic;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using System.IO;
using OpenQA.Selenium;
using OpenQA.Selenium.Firefox;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Newtonsoft.Json;
using NLog;

namespace czytanie_www_v02
{
    class Program
    {
        static string[] Scopes = { SheetsService.Scope.Spreadsheets };
        static string ApplicationName = "Google Sheets API .NET Quickstart";
        static String spreadsheetId = "spreadsheetId";
        private static readonly Logger _loger = LogManager.GetLogger("mojeLogowanieNazwa");

        static async Task Main()
        {
            var teraz = DateTime.Now.TimeOfDay;
            _loger.Info("Start aplikacji");
            while (true)
            {
                teraz = DateTime.Now.TimeOfDay;
                if ((teraz.Minutes == 07) && (teraz.Seconds == 0))
                {
                    Console.WriteLine(teraz + "Pobieram dane \n");
                    _loger.Info("Zaczyna pobierać dane");
                    await pobierz_dane();
                    _loger.Info("Koniec pobierania danych");
                }

                if ((teraz.Minutes == 12) && (teraz.Seconds == 00))
                {
                    policz_srednia_dzienna();
                    policz_srednia_tygodniowa();
                    policz_srednia_miesiaca();
                    Console.WriteLine("\n\n\nOstatnie pobranie danych : " + teraz);
                }
                await Task.Delay(1000);
            }
        }

        private static void policz_srednia_dzienna()
        {
            var firmy = new[] { "TMCE", "TUDOR", "Mierniczy" };
            for (int i = 0; i < 7; i++)
            {
                foreach (var firma in firmy)
                {
                    wstaw_dane_do_arkusza(firma, DateTime.Now.Date.AddDays(-i).ToString("yyyy-MM-dd"), i);
                }
            }
        }

        public static void wstaw_dane_do_arkusza(string firma, string data_dzien_fun, int dni_przed)
        {
            var cs = "Host=__host_name__;Username=___user_name_;Password=__passwd__;Database=__db_name__";
            using var con = new NpgsqlConnection(cs);
            con.Open();
            var cmd = new NpgsqlCommand($"drop view if exists tmp_dzienne; create view tmp_dzienne as select * from public.zajetosc_klucza where czas_pom::varchar like '{data_dzien_fun}%' and firma like '{firma}';", con);
            cmd.ExecuteNonQuery();
            cmd = new NpgsqlCommand($@"select sum_godz, licz_ip, round((sum_godz / licz_ip)::decimal, 2) as srednia_licz_godz from(
                           select distinct
                           (select  sum(licz_godz_na_ip)::float as sum_godz from(
                               select count(komp_name||login_name)::int as licz_godz_na_ip from tmp_dzienne group by komp_name||login_name
                           ) as foo) as sum_godz,
                           (select count(*)::int as licz_ip from(
                               select distinct count(komp_name||login_name), komp_name||login_name, '{data_dzien_fun}'::varchar as data_pom from tmp_dzienne group by komp_name||login_name
                           ) as foo1) as licz_ip
                        from tmp_dzienne
                    ) as foo2", con);
            using var dr = cmd.ExecuteReader();

            var row = firma switch
            {
                "TUDOR" => 5 + (dni_przed * 3),
                "TMCE" => 4 + (dni_przed * 3),
                "Mierniczy" => 6 + (dni_przed * 3),
                _ => throw new ArgumentException("Nieznana firma")
            };

            wpisz_dane_do_arkusza_google(data_dzien_fun, $"srednie!C{row}");
            wpisz_dane_do_arkusza_google("0", $"srednie!D{row}");
            wpisz_dane_do_arkusza_google("0", $"srednie!E{row}");
            while (dr.Read())
            {
                Console.WriteLine($"{dr[0]}--{dr[1]}--{dr[2]}");
                wpisz_dane_do_arkusza_google(dr[1].ToString(), $"srednie!D{row}");
                wpisz_dane_do_arkusza_google(dr[2].ToString(), $"srednie!E{row}");
            }
        }

        private static void policz_srednia_tygodniowa()
        {
            string data_dzien = DateTime.Now.Date.ToString("yyyy-MM-dd");
            Console.WriteLine(data_dzien);
            var cs = "Host=__host_name__;Username=___user_name_;Password=__passwd__;Database=__db_name__";
            var firmy = new[] { "TUDOR", "TMCE", "Mierniczy" };

            foreach (var firma in firmy)
            {
                using var con = new NpgsqlConnection(cs);
                con.Open();
                var licz_dni = (int)DateTime.Now.DayOfWeek + 1;
                var cmd = new NpgsqlCommand($"drop view if exists tmp_od_pocz_tygodnia_{firma.ToLower()}; create view tmp_od_pocz_tygodnia_{firma.ToLower()} as select * from public.zajetosc_klucza where czas_pom::date >= (now()::date - INTERVAL '{licz_dni - 1} DAY') and czas_pom::date <= now()::date and firma like '{firma}';", con);
                cmd.ExecuteNonQuery();
                cmd = new NpgsqlCommand($@"select sum_godz, licz_ip, round(((sum_godz / licz_ip)/{licz_dni})::decimal, 2) as srednia_licz_godz from(
                           select distinct
                           (select  sum(licz_godz_na_ip)::float as sum_godz from(
                               select count(komp_name||login_name)::int as licz_godz_na_ip from tmp_od_pocz_tygodnia_{firma.ToLower()} group by komp_name||login_name
                           ) as foo) as sum_godz,
                           (select count(*)::int as licz_ip from(
                               select distinct count(komp_name||login_name), komp_name||login_name, now()::date as data_pom from tmp_od_pocz_tygodnia_{firma.ToLower()} group by komp_name||login_name
                           ) as foo1) as licz_ip
                        from tmp_od_pocz_tygodnia_{firma.ToLower()}
                    ) as foo2", con);
                using var dr = cmd.ExecuteReader();

                var row = firma switch
                {
                    "TUDOR" => 6,
                    "TMCE" => 5,
                    "Mierniczy" => 7,
                    _ => throw new ArgumentException("Nieznana firma")
                };

                wpisz_dane_do_arkusza_google(data_dzien, $"srednie!I{row}");
                wpisz_dane_do_arkusza_google("0", $"srednie!J{row}");
                wpisz_dane_do_arkusza_google("0", $"srednie!K{row}");
                while (dr.Read())
                {
                    Console.WriteLine($"{dr[0]}--{dr[1]}--{dr[2]}");
                    wpisz_dane_do_arkusza_google(dr[1].ToString(), $"srednie!J{row}");
                    wpisz_dane_do_arkusza_google(dr[2].ToString(), $"srednie!K{row}");
                }
            }
        }

        private static void policz_srednia_miesiaca()
        {
            string data_dzien = DateTime.Now.Date.ToString("yyyy-MM-dd");
            Console.WriteLine(data_dzien);
            var cs = "Host=__host_name__;Username=___user_name_;Password=__passwd__;Database=__db_name__";
            var firmy = new[] { "TUDOR", "TMCE", "Mierniczy" };

            foreach (var firma in firmy)
            {
                using var con = new NpgsqlConnection(cs);
                con.Open();
                var cmd = new NpgsqlCommand($"drop view if exists tmp_od_pocz_mies_{firma.ToLower()}; create view tmp_od_pocz_mies_{firma.ToLower()} as select * from public.zajetosc_klucza where date_trunc('month', czas_pom) = date_trunc('month', now()) and firma like '{firma}';", con);
                cmd.ExecuteNonQuery();
                cmd = new NpgsqlCommand($@"select sum_godz, licz_ip, round(((sum_godz / licz_ip)/(SELECT EXTRACT(DAY from now())))::decimal, 2) as srednia_licz_godz from(
                           select distinct
                           (select  sum(licz_godz_na_ip)::float as sum_godz from(
                               select count(komp_name||login_name)::int as licz_godz_na_ip from tmp_od_pocz_mies_{firma.ToLower()} group by komp_name||login_name
                           ) as foo) as sum_godz,
                           (select count(*)::int as licz_ip from(
                               select distinct count(komp_name||login_name), komp_name||login_name, now()::date as data_pom from tmp_od_pocz_mies_{firma.ToLower()} group by komp_name||login_name
                           ) as foo1) as licz_ip
                        from tmp_od_pocz_mies_{firma.ToLower()}
                    ) as foo2", con);
                using var dr = cmd.ExecuteReader();

                var row = firma switch
                {
                    "TUDOR" => 12,
                    "TMCE" => 11,
                    "Mierniczy" => 13,
                    _ => throw new ArgumentException("Nieznana firma")
                };

                wpisz_dane_do_arkusza_google(data_dzien, $"srednie!I{row}");
                wpisz_dane_do_arkusza_google("0", $"srednie!J{row}");
                wpisz_dane_do_arkusza_google("0", $"srednie!K{row}");
                while (dr.Read())
                {
                    Console.WriteLine($"{dr[0]}--{dr[1]}--{dr[2]}");
                    wpisz_dane_do_arkusza_google(dr[1].ToString(), $"srednie!J{row}");
                    wpisz_dane_do_arkusza_google(dr[2].ToString(), $"srednie!K{row}");
                }
            }
        }

        private static void wpisz_dane_do_arkusza_google(string insert_data, string range)
        {
            try
            {
                Thread.Sleep(1000);
                UserCredential credential;
                using (var stream = new FileStream("client_secret_1055944470095-lmmmsuam2h7fetlh4sk27aeco4jlj289.apps.googleusercontent.com.json", FileMode.Open, FileAccess.ReadWrite))
                {
                    string credPath = "token.json";
                    credential = GoogleWebAuthorizationBroker.AuthorizeAsync(
                        GoogleClientSecrets.FromStream(stream).Secrets,
                        Scopes,
                        "user",
                        CancellationToken.None,
                        new FileDataStore(credPath, false)).Result;
                }

                var service = new SheetsService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = ApplicationName,
                });

                var requestBody = new ValueRange
                {
                    Values = new List<IList<object>> { new List<object> { insert_data } }
                };

                var request = service.Spreadsheets.Values.Update(requestBody, spreadsheetId, range);
                request.ValueInputOption = SpreadsheetsResource.ValuesResource.UpdateRequest.ValueInputOptionEnum.USERENTERED;
                var response = request.Execute();
                _loger.Info("Odp ser google:" + response);
            }
            catch (Exception ex)
            {
                _loger.Error(ex);
            }
        }

        private static async Task pobierz_dane()
        {
            try
            {
                _loger.Info("usuchamia FF");
                using IWebDriver driver = new FirefoxDriver();
                driver.Navigate().GoToUrl("http://ntk:ntk@192.168.0.190:40080/status");
                var cs = "Host=__host_name__;Username=___user_name_;Password=__passwd__;Database=__db_name__";
                using var con = new NpgsqlConnection(cs);
                con.Open();
                var subs = driver.PageSource.Split("<br>");
                _loger.Info("Otwarcie połącznienia do Postgresa");
                _loger.Info("Zczytanie danych ze strony");

                foreach (var sub in subs)
                {
                    var line = sub.TrimStart();
                    if (line.StartsWith("192."))
                    {
                        var parts = line.Split(new[] { "):", "(", ";" }, StringSplitOptions.None);
                        var adr_ip = parts[0];
                        var comp_name = parts[1];
                        var path_to_TM = parts[2];
                        var log_name = parts[3];
                        var TM_key = parts[4];
                        var firma = GetFirma(comp_name, log_name);

                        _loger.Info($"Zczytane dane {adr_ip} --- {comp_name} --- {path_to_TM} --- {log_name} --- {TM_key}");
                        var cmd = new NpgsqlCommand($"INSERT INTO public.zajetosc_klucza (ip, komp_name, sciezka, login_name, klucz, czas_pom, firma) VALUES ('{adr_ip}', '{comp_name}', '{path_to_TM}', '{log_name}', '{TM_key}', now(),'{firma}');", con);
                        cmd.ExecuteNonQuery();
                        await dodaj_dane_do_arkusza(adr_ip, comp_name, path_to_TM, log_name, TM_key, $"{comp_name}{log_name}", firma);
                    }
                }
                _loger.Info("Zamkniete FF");
            }
            catch (Exception ex)
            {
                _loger.Error(ex);
                throw;
            }
        }

        private static string GetFirma(string comp_name, string log_name)
        {
            return (comp_name + log_name) switch
            {
                "IOAgnieszka Żyznowska" => "TMCE",
                "LAPTOP-EKM1GVK0Agnieszka Żyznowska" => "TMCE",
                "VM-DSK-DBYDamian Bys" => "TMCE",
                "DESKTOP-AO8KIRKuser" => "TMCE",
                "DESKTOP-2K4KGEUTMCE" => "TMCE",
                "LAPTOP-FG1LMRA6Ziomek" => "TMCE",
                "LAPTOP-FG1LMRA6Asia" => "TMCE",
                "DESKTOP-8QKIU0APiotrek" => "TMCE",
                "LAPTOP-OU4933RG48502" => "TMCE",
                "DESKTOP-1TEP0R5Dell" => "TMCE",
                "AdminAdmin" => "TMCE",
                "Dell-KomputerDell" => "TMCE",
                "VM-DSK-JSZJulia Szumska" => "TMCE",
                "CADOMAIuser" => "TMCE",
                "DESKTOP-0S1K5O4USER" => "TMCE",
                "DESKTOP-ODPHGP6User" => "TMCE",
                "DESKTOP-0S1K5O4user" => "TMCE",
                "DESKTOP-ODPHGP6user" => "TMCE",
                "VM-DSK-MGAMarcin Galek" => "TMCE",
                "VM-DSK-DBADaniel Baran" => "TMCE",
                _ => log_name switch
                {
                    "tudor" => "TUDOR",
                    "rudor" => "TUDOR",
                    _ => "Mierniczy"
                }
            };
        }

        private static async Task dodaj_dane_do_arkusza(string adr_ip, string comp_name, string path_to_TM, string log_name, string TM_key, string komp_name_login_name, string firma)
        {
            try
            {
                await Task.Delay(1000);

                var oblist = new List<object>
                {
                    adr_ip,
                    comp_name,
                    path_to_TM,
                    log_name,
                    TM_key,
                    DateTime.Now.Date.ToString("yyyy-MM-dd"),
                    DateTime.Now.ToString("HH-mm"),
                    komp_name_login_name,
                    firma
                };

                var requestBody = new ValueRange
                {
                    Values = new List<IList<object>> { oblist }
                };

                UserCredential credential;
                using (var stream = new FileStream("client_secret_1055944470095-lmmmsuam2h7fetlh4sk27aeco4jlj289.apps.googleusercontent.com.json", FileMode.Open, FileAccess.ReadWrite))
                {
                    string credPath = "token.json";
                    credential = await GoogleWebAuthorizationBroker.AuthorizeAsync(
                        GoogleClientSecrets.FromStream(stream).Secrets,
                        Scopes,
                        "user",
                        CancellationToken.None,
                        new FileDataStore(credPath, false));
                }

                var service = new SheetsService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = ApplicationName,
                });

                var range = "Lista_wpisow!A1:I";
                var request = service.Spreadsheets.Values.Append(requestBody, spreadsheetId, range);
                request.InsertDataOption = SpreadsheetsResource.ValuesResource.AppendRequest.InsertDataOptionEnum.INSERTROWS;
                request.ValueInputOption = SpreadsheetsResource.ValuesResource.AppendRequest.ValueInputOptionEnum.RAW;

                var response = await request.ExecuteAsync();
                _loger.Info("Odp ser google:" + response);
            }
            catch (Exception ex)
            {
                _loger.Error(ex);
                throw;
            }
        }
    }
}