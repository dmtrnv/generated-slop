# See https://aka.ms/customizecontainer to learn how to customize your debug container
# and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["Protos/Protos.csproj", "Protos/"]
COPY ["AbonentClient/AbonentClient.csproj", "AbonentClient/"]
COPY ["OrderClient/OrderClient.csproj", "OrderClient/"]
COPY ["AdapterFacade/AdapterFacade.csproj", "AdapterFacade/"]
RUN dotnet restore "AdapterFacade/AdapterFacade.csproj"
COPY . .
WORKDIR "/src/AdapterFacade"
RUN dotnet publish "AdapterFacade.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
EXPOSE 8080
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "AdapterFacade.dll"]
