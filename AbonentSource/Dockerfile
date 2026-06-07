# See https://aka.ms/customizecontainer to learn how to customize your debug container
# and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["Protos/Protos.csproj", "Protos/"]
COPY ["AbonentSource/AbonentSource.csproj", "AbonentSource/"]
RUN dotnet restore "AbonentSource/AbonentSource.csproj"
COPY . .
WORKDIR "/src/AbonentSource"
RUN dotnet publish "AbonentSource.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
EXPOSE 8080
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "AbonentSource.dll"]
