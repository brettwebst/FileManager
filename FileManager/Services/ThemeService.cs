﻿using MudBlazor;

namespace FileManager.Services
{
    public class ThemeService
    {
        
        public MudTheme CurrentTheme = new MudTheme();
        public MudTheme DefaultTheme = new MudTheme();
        //{
        //    Palette = new Palette()
        //    {
        //        Black = "#272c34"
        //    }
        //};


        public void ToggleMode()
        {
            if (CurrentTheme == DefaultTheme)
            {
                CurrentTheme = DarkTheme;
            }
            else
            {
                CurrentTheme = DefaultTheme;
            }
        }

        public string ToggleModeIcon()
        {
            if (CurrentTheme == DefaultTheme)
            {
                return Icons.Filled.DarkMode;
            }
            else
            {
                return Icons.Filled.LightMode;
            }
        }

        MudTheme DarkTheme = new MudTheme()
        { 
            Palette = new Palette()
            {
                Black = "#27272f",
                Background = "#32333d",
                BackgroundGrey = "#27272f",
                Surface = "#373740",
                DrawerBackground = "#27272f",
                DrawerText = "rgba(255,255,255, 0.50)",
                DrawerIcon = "rgba(255,255,255, 0.50)",
                AppbarBackground = "#27272f",
                AppbarText = "rgba(255,255,255, 0.70)",
                TextPrimary = "rgba(255,255,255, 0.70)",
                TextSecondary = "rgba(255,255,255, 0.50)",
                ActionDefault = "#adadb1",
                ActionDisabled = "rgba(255,255,255, 0.26)",
                ActionDisabledBackground = "rgba(255,255,255, 0.12)",
                Divider = "rgba(255,255,255, 0.12)",
                DividerLight = "rgba(255,255,255, 0.06)",
                TableLines = "rgba(255,255,255, 0.12)",
                LinesDefault = "rgba(255,255,255, 0.12)",
                LinesInputs = "rgba(255,255,255, 0.3)",
                TextDisabled = "rgba(255,255,255, 0.2)"
            }
        };
    }
}
